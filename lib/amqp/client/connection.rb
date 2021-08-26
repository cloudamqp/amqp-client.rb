# frozen_string_literal: true

require "socket"
require "uri"
require "openssl"
require_relative "./frames"
require_relative "./channel"
require_relative "./errors"

module AMQP
  # Represents a single AMQP connection
  class Connection
    def self.connect(uri, read_loop_thread: true, **options)
      uri = URI.parse(uri)
      tls = uri.scheme == "amqps"
      port = port_from_env || uri.port || (tls ? 5671 : 5672)
      host = uri.host || "localhost"
      user = uri.user || "guest"
      password = uri.password || "guest"
      vhost = URI.decode_www_form_component(uri.path[1..-1] || "/")
      options = URI.decode_www_form(uri.query || "").map! { |k, v| [k.to_sym, v] }.to_h.merge(options)

      socket = Socket.tcp host, port, connect_timeout: 20, resolv_timeout: 5
      enable_tcp_keepalive(socket)
      if tls
        cert_store = OpenSSL::X509::Store.new
        cert_store.set_default_paths
        context = OpenSSL::SSL::SSLContext.new
        context.cert_store = cert_store
        context.verify_mode = OpenSSL::SSL::VERIFY_PEER unless [false, "false", "none"].include? options[:verify_peer]
        socket = OpenSSL::SSL::SSLSocket.new(socket, context)
        socket.sync_close = true # closing the TLS socket also closes the TCP socket
        socket.hostname = host # SNI host
        socket.connect
      end
      channel_max, frame_max, heartbeat = establish(socket, user, password, vhost, **options)
      Connection.new(socket, channel_max, frame_max, heartbeat, read_loop_thread: read_loop_thread)
    end

    def initialize(socket, channel_max, frame_max, heartbeat, read_loop_thread: true)
      @socket = socket
      @channel_max = channel_max.zero? ? 65_536 : channel_max
      @frame_max = frame_max
      @heartbeat = heartbeat
      @channels = {}
      @closed = false
      @replies = Queue.new
      Thread.new { read_loop } if read_loop_thread
    end

    attr_reader :frame_max

    def inspect
      "#<#{self.class} @closed=#{@closed} channel_count=#{@channels.size}>"
    end

    def channel(id = nil)
      raise ArgumentError, "Channel ID cannot be 0" if id&.zero?
      raise ArgumentError, "Channel ID higher than connection's channel max #{@channel_max}" if id && id > @channel_max

      if id
        ch = @channels[id] ||= Channel.new(self, id)
      else
        id = nil
        1.upto(@channel_max) do |i|
          break id = i unless @channels.key? i
        end
        raise AMQP::Client::Error, "Max channels reached" if id.nil?

        ch = @channels[id] = Channel.new(self, id)
      end
      ch.open
    end

    # Declare a new channel, yield, and then close the channel
    def with_channel
      ch = channel
      begin
        yield ch
      ensure
        ch.close
      end
    end

    def close(reason = "", code = 200)
      return if @closed

      @closed = true
      write_bytes FrameBytes.connection_close(code, reason)
      @channels.each_value { |ch| ch.closed!(code, reason, 0, 0) }
      expect(:close_ok)
    end

    def closed?
      @closed
    end

    def write_bytes(*bytes)
      @socket.write(*bytes)
    rescue IOError, OpenSSL::OpenSSLError, SystemCallError => e
      raise AMQP::Client::Error, "Could not write to socket, #{e.message}"
    end

    # Reads from the socket, required for any kind of progress. Blocks until the connection is closed
    def read_loop
      # read more often than write so that channel errors crop up early
      Thread.current.priority += 1
      socket = @socket
      frame_max = @frame_max
      frame_start = String.new(capacity: 7)
      frame_buffer = String.new(capacity: frame_max)
      loop do
        socket.read(7, frame_start)
        type, channel_id, frame_size = frame_start.unpack("C S> L>")
        if frame_size > frame_max
          raise AMQP::Client::Error, "Frame size #{frame_size} is larger than negotiated max frame size #{frame_max}"
        end

        # read the frame content
        socket.read(frame_size, frame_buffer)

        # make sure that the frame end is correct
        frame_end = socket.readchar.ord
        raise AMQP::Client::UnexpectedFrameEnd, frame_end if frame_end != 206

        # parse the frame, will return false if a close frame was received
        parse_frame(type, channel_id, frame_size, frame_buffer) || return
      end
    rescue IOError, OpenSSL::OpenSSLError, SystemCallError => e
      warn "AMQP-Client read error: #{e.inspect}"
      nil # ignore read errors
    ensure
      @closed = true
      @replies.close
      begin
        @socket.close
      rescue IOError, OpenSSL::OpenSSLError, SystemCallError
        nil
      end
    end

    private

    def parse_frame(type, channel_id, frame_size, buf)
      case type
      when 1 # method frame
        class_id, method_id = buf.unpack("S> S>")
        case class_id
        when 10 # connection
          raise AMQP::Client::Error, "Unexpected channel id #{channel_id} for Connection frame" if channel_id != 0

          case method_id
          when 50 # connection#close
            @closed = true
            code, text_len = buf.unpack("@4 S> C")
            text = buf.byteslice(7, text_len).force_encoding("utf-8")
            error_class_id, error_method_id = buf.byteslice(7 + text_len, 4).unpack("S> S>")
            @channels.each_value { |ch| ch.closed!(code, text, error_class_id, error_method_id) }
            begin
              write_bytes FrameBytes.connection_close_ok
            rescue AMQP::Client::Error
              nil # rabbitmq closes the socket after sending Connection::Close, so ignore write errors
            end
            return false
          when 51 # connection#close-ok
            @closed = true
            @replies.push [:close_ok]
            return false
          else raise AMQP::Client::UnsupportedMethodFrame, class_id, method_id
          end
        when 20 # channel
          case method_id
          when 11 # channel#open-ok
            @channels[channel_id].reply [:channel_open_ok]
          when 40 # channel#close
            reply_code, reply_text_len = buf.unpack("@4 S> C")
            reply_text = buf.byteslice(7, reply_text_len).force_encoding("utf-8")
            classid, methodid = buf.byteslice(7 + reply_text_len, 4).unpack("S> S>")
            channel = @channels.delete(channel_id)
            channel.closed!(reply_code, reply_text, classid, methodid)
            write_bytes FrameBytes.channel_close_ok(channel_id)
          when 41 # channel#close-ok
            channel = @channels.delete(channel_id)
            channel.reply [:channel_close_ok]
          else raise AMQP::Client::UnsupportedMethodFrame, class_id, method_id
          end
        when 40 # exchange
          case method_id
          when 11 # declare-ok
            @channels[channel_id].reply [:exchange_declare_ok]
          when 21 # delete-ok
            @channels[channel_id].reply [:exchange_delete_ok]
          when 31 # bind-ok
            @channels[channel_id].reply [:exchange_bind_ok]
          when 51 # unbind-ok
            @channels[channel_id].reply [:exchange_unbind_ok]
          else raise AMQP::Client::UnsupportedMethodFrame, class_id, method_id
          end
        when 50 # queue
          case method_id
          when 11 # declare-ok
            queue_name_len = buf.unpack1("@4 C")
            queue_name = buf.byteslice(5, queue_name_len).force_encoding("utf-8")
            message_count, consumer_count = buf.byteslice(5 + queue_name_len, 8).unpack("L> L>")
            @channels[channel_id].reply [:queue_declare_ok, queue_name, message_count, consumer_count]
          when 21 # bind-ok
            @channels[channel_id].reply [:queue_bind_ok]
          when 31 # purge-ok
            @channels[channel_id].reply [:queue_purge_ok]
          when 41 # delete-ok
            message_count = buf.unpack1("@4 L>")
            @channels[channel_id].reply [:queue_delete, message_count]
          when 51 # unbind-ok
            @channels[channel_id].reply [:queue_unbind_ok]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        when 60 # basic
          case method_id
          when 11 # qos-ok
            @channels[channel_id].reply [:basic_qos_ok]
          when 21 # consume-ok
            tag_len = buf.unpack1("@4 C")
            tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
            @channels[channel_id].reply [:basic_consume_ok, tag]
          when 30 # cancel
            tag_len = buf.unpack1("@4 C")
            tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
            no_wait = buf[5 + tag_len].ord
            @channels[channel_id].close_consumer(tag)
            write_bytes FrameBytes.basic_cancel_ok(@id, tag) unless no_wait == 1
          when 31 # cancel-ok
            tag_len = buf.unpack1("@4 C")
            tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
            @channels[channel_id].reply [:basic_cancel_ok, tag]
          when 50 # return
            reply_code, reply_text_len = buf.unpack("@4 S> C")
            pos = 7
            reply_text = buf.byteslice(pos, reply_text_len).force_encoding("utf-8")
            pos += reply_text_len
            exchange_len = buf[pos].ord
            pos += 1
            exchange = buf.byteslice(pos, exchange_len).force_encoding("utf-8")
            pos += exchange_len
            routing_key_len = buf[pos].ord
            pos += 1
            routing_key = buf.byteslice(pos, routing_key_len).force_encoding("utf-8")
            @channels[channel_id].message_returned(reply_code, reply_text, exchange, routing_key)
          when 60 # deliver
            ctag_len = buf[4].ord
            consumer_tag = buf.byteslice(5, ctag_len).force_encoding("utf-8")
            pos = 5 + ctag_len
            delivery_tag, redelivered, exchange_len = buf.byteslice(pos, 10).unpack("Q> C C")
            pos += 8 + 1 + 1
            exchange = buf.byteslice(pos, exchange_len).force_encoding("utf-8")
            pos += exchange_len
            rk_len = buf[pos].ord
            pos += 1
            routing_key = buf.byteslice(pos, rk_len).force_encoding("utf-8")
            @channels[channel_id].message_delivered(consumer_tag, delivery_tag, redelivered == 1, exchange, routing_key)
          when 71 # get-ok
            delivery_tag, redelivered, exchange_len = buf.unpack("@4 Q> C C")
            pos = 14
            exchange = buf.byteslice(pos, exchange_len).force_encoding("utf-8")
            pos += exchange_len
            routing_key_len = buf[pos].ord
            pos += 1
            routing_key = buf.byteslice(pos, routing_key_len).force_encoding("utf-8")
            pos += routing_key_len
            _message_count = buf.byteslice(pos, 4).unpack1("L>")
            @channels[channel_id].message_delivered(nil, delivery_tag, redelivered == 1, exchange, routing_key)
          when 72 # get-empty
            @channels[channel_id].basic_get_empty
          when 80 # ack
            delivery_tag, multiple = buf.unpack("@4 Q> C")
            @channels[channel_id].confirm [:ack, delivery_tag, multiple == 1]
          when 111 # recover-ok
            @channels[channel_id].reply [:basic_recover_ok]
          when 120 # nack
            delivery_tag, multiple, requeue = buf.unpack("@4 Q> C C")
            @channels[channel_id].confirm [:nack, delivery_tag, multiple == 1, requeue == 1]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        when 85 # confirm
          case method_id
          when 11 # select-ok
            @channels[channel_id].reply [:confirm_select_ok]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        when 90 # tx
          case method_id
          when 11 # select-ok
            @channels[channel_id].reply [:tx_select_ok]
          when 21 # commit-ok
            @channels[channel_id].reply [:tx_commit_ok]
          when 31 # rollback-ok
            @channels[channel_id].reply [:tx_rollback_ok]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
        end
      when 2 # header
        body_size = buf.unpack1("@4 Q>")
        properties = Properties.decode(buf.byteslice(12, buf.bytesize - 12))
        @channels[channel_id].header_delivered body_size, properties
      when 3 # body
        @channels[channel_id].body_delivered buf
      else raise AMQP::Client::UnsupportedFrameType, type
      end
      true
    end

    def expect(expected_frame_type)
      frame_type, args = @replies.pop
      frame_type == expected_frame_type || raise(AMQP::Client::UnexpectedFrame.new(expected_frame_type, frame_type))
      args
    end

    def self.establish(socket, user, password, vhost, **options)
      channel_max, frame_max, heartbeat = nil
      socket.write "AMQP\x00\x00\x09\x01"
      buf = String.new(capacity: 4096)
      loop do
        begin
          socket.readpartial(4096, buf)
        rescue IOError, OpenSSL::OpenSSLError, SystemCallError => e
          raise AMQP::Client::Error, "Could not establish AMQP connection: #{e.message}"
        end

        type, channel_id, frame_size = buf.unpack("C S> L>")
        frame_end = buf[frame_size + 7].ord
        raise UnexpectedFrameEndError, frame_end if frame_end != 206

        case type
        when 1 # method frame
          class_id, method_id = buf.unpack("@7 S> S>")
          case class_id
          when 10 # connection
            raise AMQP::Client::Error, "Unexpected channel id #{channel_id} for Connection frame" if channel_id != 0

            case method_id
            when 10 # connection#start
              conn_name = options[:connection_name] || $PROGRAM_NAME
              properties = CLIENT_PROPERTIES.merge({ connection_name: conn_name })
              socket.write FrameBytes.connection_start_ok "\u0000#{user}\u0000#{password}", properties
            when 30 # connection#tune
              channel_max, frame_max, heartbeat = buf.unpack("@11 S> L> S>")
              channel_max = [channel_max, 2048].min
              frame_max = [frame_max, 131_072].min
              heartbeat = [heartbeat, 0].min
              socket.write FrameBytes.connection_tune_ok(channel_max, frame_max, heartbeat)
              socket.write FrameBytes.connection_open(vhost)
            when 41 # connection#open-ok
              return [channel_max, frame_max, heartbeat]
            when 50 # connection#close
              code, text_len = buf.unpack("@11 S> C")
              text, error_class_id, error_method_id = buf.unpack("@14 a#{text_len} S> S>")
              socket.write FrameBytes.connection_close_ok
              raise AMQP::Client::Error, "Could not establish AMQP connection: #{code} #{text} #{error_class_id} #{error_method_id}"
            else raise AMQP::Client::Error, "Unexpected class/method: #{class_id} #{method_id}"
            end
          else raise AMQP::Client::Error, "Unexpected class/method: #{class_id} #{method_id}"
          end
        else raise AMQP::Client::Error, "Unexpected frame type: #{type}"
        end
      end
    rescue StandardError
      socket.close rescue nil
      raise
    end

    def self.enable_tcp_keepalive(socket)
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, true)
      socket.setsockopt(Socket::SOL_TCP, Socket::TCP_KEEPIDLE, 60)
      socket.setsockopt(Socket::SOL_TCP, Socket::TCP_KEEPINTVL, 10)
      socket.setsockopt(Socket::SOL_TCP, Socket::TCP_KEEPCNT, 3)
    rescue StandardError => e
      warn "AMQP-Client could not enable TCP keepalive on socket. #{e.inspect}"
    end

    def self.port_from_env
      return unless (port = ENV["AMQP_PORT"])

      port.to_i
    end

    private_class_method :establish, :enable_tcp_keepalive, :port_from_env

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
      version: AMQP::Client::VERSION,
      information: "http://github.com/cloudamqp/amqp-client.rb"
    }.freeze
  end
end
