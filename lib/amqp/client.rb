# frozen_string_literal: true

require "socket"
require "uri"
require "openssl"
require "set"
require_relative "client/version"
require_relative "client/errors"
require_relative "client/frames"
require_relative "client/connection"
require_relative "client/channel"

module AMQP
  # AMQP 0-9-1 Client
  class Client
    def initialize(uri, **options)
      @uri = URI.parse(uri)
      @tls = @uri.scheme == "amqps"
      @port = port_from_env || @uri.port || (@tls ? 5671 : 5672)
      @host = @uri.host || "localhost"
      @user = @uri.user || "guest"
      @password = @uri.password || "guest"
      @vhost = URI.decode_www_form_component(@uri.path[1..-1] || "/")
      @options = URI.decode_www_form(@uri.query || "").to_h.merge(options)

      @queues = {}
      @subscriptions = Set.new
      @connq = SizedQueue.new(1)
    end

    def connect
      socket = Socket.tcp @host, @port, connect_timeout: 20, resolv_timeout: 5
      enable_tcp_keepalive(socket)
      if @tls
        context = OpenSSL::SSL::SSLContext.new
        context.verify_mode = OpenSSL::SSL::VERIFY_PEER unless @options["verify_peer"] == "none"
        socket = OpenSSL::SSL::SSLSocket.new(socket, context)
        socket.sync_close = true # closing the TLS socket also closes the TCP socket
      end
      channel_max, frame_max, heartbeat = establish(socket)
      Connection.new(socket, channel_max, frame_max, heartbeat)
    end

    def queue(name)
      q = @queues[name]
      unless q
        q = @queues[name] = Queue.new(self, name)
        with_connection do |conn|
          conn.channel(1).queue_declare(name)
        end
      end
      q
    end

    def start
      @connq << connect
    end

    def stop
      conn = @connq.pop
      conn.close
    end

    def subscribe(queue_name, prefetch: 1, arguments: {}, &blk)
      @subscriptions.add? [queue_name, prefetch, arguments, blk]

      with_connection do |conn|
        ch = conn.channel
        ch.basic_qos(prefetch)
        ch.basic_consume(queue_name, no_ack: false, arguments: arguments) do |msg|
          blk.call(msg)
          ch.basic_ack msg.delivery_tag
        rescue => e
          warn "AMQP-Client subscribe #{queue_name} error: #{e.inspect}"
          sleep 1
          ch.basic_reject msg.delivery_tag, requeue: true
        end
      end
    end

    def publish(body, exchange, routing_key, **properties)
      loop do
        with_connection do |conn|
          # Use channel 1 for publishes
          conn.channel(1).basic_publish_confirm(body, exchange, routing_key, **properties)
        end
        return
      rescue => e
        warn "AMQP-Client error publishing, retrying (#{e.inspect})"
      end
    end

    def bind(queue, exchange, routing_key, **headers)
      with_connection do |conn|
        conn.channel(1).queue_bind(queue, exchange, routing_key, **headers)
      end
    end

    def unbind(queue, exchange, routing_key, **headers)
      with_connection do |conn|
        conn.channel(1).queue_unbind(queue, exchange, routing_key, **headers)
      end
    end

    def purge(queue)
      with_connection do |conn|
        conn.channel(1).queue_purge(queue)
      end
    end

    def delete_queue(queue)
      with_connection do |conn|
        conn.channel(1).queue_delete(queue)
      end
    end

    # Queue abstraction
    class Queue
      def initialize(client, name)
        @client = client
        @name = name
      end

      def publish(body, **properties)
        @client.publish(body, "", @name, **properties)
        self
      end

      def subscribe(prefetch: 1, arguments: {}, &blk)
        @client.subscribe(@name, prefetch: prefetch, arguments: arguments, &blk)
        self
      end

      def bind(exchange, routing_key, **headers)
        @client.bind(@name, exchange, routing_key, **headers)
        self
      end

      def unbind(exchange, routing_key, **headers)
        @client.unbind(@name, exchange, routing_key, **headers)
        self
      end

      def purge
        @client.purge(@name)
        self
      end

      def delete
        @client.delete_queue(@name)
        nil
      end
    end

    private

    def with_connection
      conn = @connq.pop
      begin
        yield conn
      ensure
        conn = reconnect if conn.closed?
        @connq << conn
      end
    end

    def reconnect
      loop do
        conn = connect
        restore_connection_state(conn)
        return conn
      rescue e
        warn "AMQP-Client reconnect error: #{e.inspect}"
        sleep 1
      end
    end

    def restore_connection_state(conn)
      conn.channel(1) # reserve channel 1 for publishes
      @subscriptions.each { |args| subscribe(*args) }
    end

    def establish(socket)
      channel_max, frame_max, heartbeat = nil
      socket.write "AMQP\x00\x00\x09\x01"
      buf = String.new(capacity: 4096)
      loop do
        begin
          socket.readpartial(4096, buf)
        rescue IOError, OpenSSL::OpenSSLError, SystemCallError => e
          raise Error, "Could not establish AMQP connection: #{e.message}"
        end

        type, channel_id, frame_size = buf.unpack("C S> L>")
        frame_end = buf.unpack1("@#{frame_size + 7} C")
        raise UnexpectedFrameEndError, frame_end if frame_end != 206

        case type
        when 1 # method frame
          class_id, method_id = buf.unpack("@7 S> S>")
          case class_id
          when 10 # connection
            raise Error, "Unexpected channel id #{channel_id} for Connection frame" if channel_id != 0

            case method_id
            when 10 # connection#start
              properties = CLIENT_PROPERTIES.merge({
                connection_name: @options[:connection_name]
              })
              socket.write FrameBytes.connection_start_ok "\u0000#{@user}\u0000#{@password}", properties
            when 30 # connection#tune
              channel_max, frame_max, heartbeat = buf.unpack("@11 S> L> S>")
              channel_max = [channel_max, 2048].min
              frame_max = [frame_max, 131_072].min
              heartbeat = [heartbeat, 0].min
              socket.write FrameBytes.connection_tune_ok(channel_max, frame_max, heartbeat)
              socket.write FrameBytes.connection_open(@vhost)
            when 41 # connection#open-ok
              return [channel_max, frame_max, heartbeat]
            when 50 # connection#close
              code, text_len = buf.unpack("@11 S> C")
              text, error_class_id, error_method_id = buf.unpack("@14 a#{text_len} S> S>")
              socket.write FrameBytes.connection_close_ok
              raise Error, "Could not establish AMQP connection: #{code} #{text} #{error_class_id} #{error_method_id}"
            else raise Error, "Unexpected class/method: #{class_id} #{method_id}"
            end
          else raise Error, "Unexpected class/method: #{class_id} #{method_id}"
          end
        else raise Error, "Unexpected frame type: #{type}"
        end
      end
    rescue StandardError
      socket.close
      raise
    end

    def enable_tcp_keepalive(socket)
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, true)
      socket.setsockopt(Socket::SOL_TCP, Socket::TCP_KEEPIDLE, 60)
      socket.setsockopt(Socket::SOL_TCP, Socket::TCP_KEEPINTVL, 10)
      socket.setsockopt(Socket::SOL_TCP, Socket::TCP_KEEPCNT, 3)
    rescue StandardError => e
      warn "amqp-client: Could not enable TCP keepalive on socket. #{e.inspect}"
    end

    def port_from_env
      return unless (port = ENV["AMQP_PORT"])

      port.to_i
    end

    CLIENT_PROPERTIES = {
      capabilities: {
        authentication_failure_close: true,
        publisher_confirms: true,
        consumer_cancel_notify: true,
        exchange_exchange_bindings: true,
        "basic.nack": true,
        "connection.blocked": true
      },
      product: "amqp-client.rb",
      platform: RUBY_DESCRIPTION,
      version: VERSION,
      information: "http://github.com/cloudamqp/amqp-client.rb"
    }.freeze
  end
end
