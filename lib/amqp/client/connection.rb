# frozen_string_literal: true

require "socket"
require "uri"
require "openssl"
require_relative "./frame_bytes"
require_relative "./channel"
require_relative "./errors"

module AMQP
  class Client
    # Represents a single established AMQP connection
    class Connection
      # Establish a connection to an AMQP broker
      # @param uri [String] URL on the format amqp://username:password@hostname/vhost, use amqps:// for encrypted connection
      # @param read_loop_thread [Boolean] If true run {#read_loop} in a background thread,
      #   otherwise the user have to run it explicitly, without {#read_loop} the connection won't function
      # @option options [Boolean] connection_name (PROGRAM_NAME) Set a name for the connection to be able to identify
      #   the client from the broker
      # @option options [Boolean] verify_peer (true) Verify broker's TLS certificate, set to false for self-signed certs
      # @option options [Float] connect_timeout (30) TCP connection timeout
      # @option options [Integer] heartbeat (0) Heartbeat timeout, defaults to 0 and relies on TCP keepalive instead
      # @option options [Integer] frame_max (131_072) Maximum frame size,
      #    the smallest of the client's and the broker's values will be used
      # @option options [Integer] channel_max (2048) Maxium number of channels the client will be allowed to have open.
      #   Maxium allowed is 65_536.  The smallest of the client's and the broker's value will be used.
      # @option options [String] keepalive (60:10:3) TCP keepalive setting, 60s idle, 10s interval between probes, 3 probes
      # @return [Connection]
      def initialize(uri = "", read_loop_thread: true, **options)
        uri = URI.parse(uri)
        tls = uri.scheme == "amqps"
        port = port_from_env || uri.port || (tls ? 5671 : 5672)
        host = uri.host || "localhost"
        user = uri.user || "guest"
        password = uri.password || "guest"
        vhost = URI.decode_www_form_component(uri.path[1..] || "/")
        options = URI.decode_www_form(uri.query || "").map! { |k, v| [k.to_sym, v] }.to_h.merge(options)

        socket = open_socket(host, port, tls, options)
        channel_max, frame_max, heartbeat = establish(socket, user, password, vhost, options)

        @socket = socket
        @channel_max = channel_max.zero? ? 65_536 : channel_max
        @frame_max = frame_max
        @heartbeat = heartbeat
        @channels = {}
        @channels_lock = Mutex.new
        @closed = nil
        @replies = ::Queue.new
        @write_lock = Mutex.new
        @blocked = nil
        @on_blocked = ->(reason) { warn "AMQP-Client blocked by broker: #{reason}" }
        @on_unblocked = -> { warn "AMQP-Client unblocked by broker" }

        # Only used with heartbeats
        @last_sent = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @last_recv = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        Thread.new { read_loop } if read_loop_thread
      end

      # Indicates that the server is blocking publishes.
      # If the client keeps publishing the server will stop reading from the socket.
      # Use the #on_blocked callback to get notified when the server is resource constrained.
      # @see #on_blocked
      # @see #on_unblocked
      # @return [Bool]
      def blocked?
        !@blocked.nil?
      end

      # Alias for {#initialize}
      # @see #initialize
      # @deprecated
      def self.connect(uri, read_loop_thread: true, **options)
        new(uri, read_loop_thread: read_loop_thread, **options)
      end

      # The max frame size negotiated between the client and the broker
      # @return [Integer]
      attr_reader :frame_max

      # Custom inspect
      # @return [String]
      # @api private
      def inspect
        "#<#{self.class} @closed=#{@closed} channel_count=#{@channels.size}>"
      end

      # Open an AMQP channel
      # @param id [Integer, nil] If nil a new channel will be opened, otherwise an already open channel might be reused
      # @return [Channel]
      def channel(id = nil)
        raise ArgumentError, "Channel ID cannot be 0" if id&.zero?
        raise ArgumentError, "Channel ID higher than connection's channel max #{@channel_max}" if id && id > @channel_max

        ch = @channels_lock.synchronize do
          if id
            @channels[id] ||= Channel.new(self, id)
          else
            1.upto(@channel_max) do |i|
              break id = i unless @channels.key? i
            end
            raise Error, "Max channels reached" if id.nil?

            @channels[id] = Channel.new(self, id)
          end
        end
        ch.open
      end

      # Declare a new channel, yield, and then close the channel
      # @yield [Channel]
      # @return [Object] Whatever was returned by the block
      def with_channel
        ch = channel
        begin
          yield ch
        ensure
          ch.close
        end
      end

      # Gracefully close a connection
      # @param reason [String] A reason to close the connection can be logged by the broker
      # @param code [Integer]
      # @return [nil]
      def close(reason: "", code: 200)
        return if @closed

        @closed = [code, reason]
        @channels.each_value { |ch| ch.closed!(:connection, code, reason, 0, 0) }
        if @blocked
          @socket.close
        else
          write_bytes FrameBytes.connection_close(code, reason)
          expect(:close_ok)
        end
        nil
      end

      # Update authentication secret, for example when an OAuth backend is used
      # @param secret [String] The new secret
      # @param reason [String] A reason to update it
      # @return [nil]
      def update_secret(secret, reason)
        write_bytes FrameBytes.update_secret(secret, reason)
        expect(:update_secret_ok)
        nil
      end

      # True if the connection is closed
      # @return [Boolean]
      def closed?
        !@closed.nil?
      end

      # @!group Callbacks

      # Callback called when client is blocked by the broker
      # @yield [String] reason to why the connection is being blocked
      # @return [nil]
      def on_blocked(&blk)
        @on_blocked = blk
        nil
      end

      # Callback called when client is unblocked by the broker
      # @yield
      # @return [nil]
      def on_unblocked(&blk)
        @on_unblocked = blk
        nil
      end

      # @!endgroup

      # Write byte array(s) directly to the socket (thread-safe)
      # @param bytes [String] One or more byte arrays
      # @return [Integer] number of bytes written
      # @api private
      def write_bytes(*bytes)
        @write_lock.synchronize do
          @socket.write(*bytes)
          update_last_sent
        end
      rescue *READ_EXCEPTIONS => e
        raise Error::ConnectionClosed.new(*@closed) if @closed

        raise Error, "Could not write to socket, #{e.message}"
      end

      # Reads from the socket, required for any kind of progress.
      # Blocks until the connection is closed. Normally run as a background thread automatically.
      # @return [nil]
      def read_loop
        # read more often than write so that channel errors crop up early
        Thread.current.priority += 1
        socket = @socket
        frame_max = @frame_max
        frame_start = String.new(capacity: 7)
        frame_buffer = String.new(capacity: frame_max)
        loop do
          socket.read(7, frame_start) || raise(IOError)
          type, channel_id, frame_size = frame_start.unpack("C S> L>")
          frame_max >= frame_size || raise(Error, "Frame size #{frame_size} larger than negotiated max frame size #{frame_max}")

          # read the frame content
          socket.read(frame_size, frame_buffer) || raise(IOError)

          # make sure that the frame end is correct
          frame_end = socket.readchar.ord
          raise Error::UnexpectedFrameEnd, frame_end if frame_end != 206

          # parse the frame, will return false if a close frame was received
          parse_frame(type, channel_id, frame_buffer) || return
          update_last_recv
        end
        nil
      rescue *READ_EXCEPTIONS => e
        @closed ||= [400, "read error: #{e.message}"]
        nil # ignore read errors
      ensure
        @closed ||= [400, "unknown"]
        @replies.close
        begin
          if @write_lock.owned? # if connection is blocked
            @socket.close
          else
            @write_lock.synchronize do
              @socket.close
            end
          end
        rescue *READ_EXCEPTIONS
          nil
        end
      end

      private

      READ_EXCEPTIONS = [IOError, OpenSSL::OpenSSLError, SystemCallError,
                         RUBY_ENGINE == "jruby" ? java.lang.NullPointerException : nil].compact.freeze

      def parse_frame(type, channel_id, buf) # rubocop:disable Metrics/MethodLength, Metrics/CyclomaticComplexity
        channel = @channels[channel_id]
        case type
        when 1 # method frame
          class_id, method_id = buf.unpack("S> S>")
          case class_id
          when 10 # connection
            raise Error, "Unexpected channel id #{channel_id} for Connection frame" if channel_id != 0

            case method_id
            when 50 # connection#close
              code, text_len = buf.unpack("@4 S> C")
              text = buf.byteslice(7, text_len).force_encoding("utf-8")
              error_class_id, error_method_id = buf.byteslice(7 + text_len, 4).unpack("S> S>")
              @closed = [code, text, error_class_id, error_method_id]
              @channels.each_value { |ch| ch.closed!(:connection, code, text, error_class_id, error_method_id) }
              begin
                write_bytes FrameBytes.connection_close_ok
              rescue Error
                nil # rabbitmq closes the socket after sending Connection::Close, so ignore write errors
              end
              return false
            when 51 # connection#close-ok
              @replies.push [:close_ok]
              return false
            when 60 # connection#blocked
              reason_len = buf.getbyte(4)
              reason = buf.byteslice(5, reason_len).force_encoding("utf-8")
              @blocked = reason
              @on_blocked.call(reason)
            when 61 # connection#unblocked
              @blocked = nil
              @on_unblocked.call
            when 71 # connection#update_secret_ok
              @replies.push [:update_secret_ok]
            else raise Error::UnsupportedMethodFrame, class_id, method_id
            end
          when 20 # channel
            case method_id
            when 11 # channel#open-ok
              channel.reply [:channel_open_ok]
            when 40 # channel#close
              reply_code, reply_text_len = buf.unpack("@4 S> C")
              reply_text = buf.byteslice(7, reply_text_len).force_encoding("utf-8")
              classid, methodid = buf.byteslice(7 + reply_text_len, 4).unpack("S> S>")
              channel = @channels_lock.synchronize { @channels.delete(channel_id) }
              channel.closed!(:channel, reply_code, reply_text, classid, methodid)
              write_bytes FrameBytes.channel_close_ok(channel_id)
            when 41 # channel#close-ok
              channel = @channels_lock.synchronize { @channels.delete(channel_id) }
              channel.reply [:channel_close_ok]
            else raise Error::UnsupportedMethodFrame, class_id, method_id
            end
          when 40 # exchange
            case method_id
            when 11 # declare-ok
              channel.reply [:exchange_declare_ok]
            when 21 # delete-ok
              channel.reply [:exchange_delete_ok]
            when 31 # bind-ok
              channel.reply [:exchange_bind_ok]
            when 51 # unbind-ok
              channel.reply [:exchange_unbind_ok]
            else raise Error::UnsupportedMethodFrame, class_id, method_id
            end
          when 50 # queue
            case method_id
            when 11 # declare-ok
              queue_name_len = buf.getbyte(4)
              queue_name = buf.byteslice(5, queue_name_len).force_encoding("utf-8")
              message_count, consumer_count = buf.byteslice(5 + queue_name_len, 8).unpack("L> L>")
              channel.reply [:queue_declare_ok, queue_name, message_count, consumer_count]
            when 21 # bind-ok
              channel.reply [:queue_bind_ok]
            when 31 # purge-ok
              message_count = buf.unpack1("@4 L>")
              channel.reply [:queue_purge_ok, message_count]
            when 41 # delete-ok
              message_count = buf.unpack1("@4 L>")
              channel.reply [:queue_delete, message_count]
            when 51 # unbind-ok
              channel.reply [:queue_unbind_ok]
            else raise Error::UnsupportedMethodFrame.new class_id, method_id
            end
          when 60 # basic
            case method_id
            when 11 # qos-ok
              channel.reply [:basic_qos_ok]
            when 21 # consume-ok
              tag_len = buf.getbyte(4)
              tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
              channel.reply [:basic_consume_ok, tag]
            when 30 # cancel
              tag_len = buf.getbyte(4)
              tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
              no_wait = buf.getbyte(5 + tag_len) == 1
              channel.close_consumer(tag)
              write_bytes FrameBytes.basic_cancel_ok(@id, tag) unless no_wait
            when 31 # cancel-ok
              tag_len = buf.getbyte(4)
              tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
              channel.reply [:basic_cancel_ok, tag]
            when 50 # return
              reply_code, reply_text_len = buf.unpack("@4 S> C")
              pos = 7
              reply_text = buf.byteslice(pos, reply_text_len).force_encoding("utf-8")
              pos += reply_text_len
              exchange_len = buf.getbyte(pos)
              pos += 1
              exchange = buf.byteslice(pos, exchange_len).force_encoding("utf-8")
              pos += exchange_len
              routing_key_len = buf.getbyte(pos)
              pos += 1
              routing_key = buf.byteslice(pos, routing_key_len).force_encoding("utf-8")
              channel.message_returned(reply_code, reply_text, exchange, routing_key)
            when 60 # deliver
              ctag_len = buf.getbyte(4)
              consumer_tag = buf.byteslice(5, ctag_len).force_encoding("utf-8")
              pos = 5 + ctag_len
              delivery_tag, redelivered, exchange_len = buf.byteslice(pos, 10).unpack("Q> C C")
              pos += 8 + 1 + 1
              exchange = buf.byteslice(pos, exchange_len).force_encoding("utf-8")
              pos += exchange_len
              rk_len = buf.getbyte(pos)
              pos += 1
              routing_key = buf.byteslice(pos, rk_len).force_encoding("utf-8")
              channel.message_delivered(consumer_tag, delivery_tag, redelivered == 1, exchange, routing_key)
            when 71 # get-ok
              delivery_tag, redelivered, exchange_len = buf.unpack("@4 Q> C C")
              pos = 14
              exchange = buf.byteslice(pos, exchange_len).force_encoding("utf-8")
              pos += exchange_len
              routing_key_len = buf.getbyte(pos)
              pos += 1
              routing_key = buf.byteslice(pos, routing_key_len).force_encoding("utf-8")
              # pos += routing_key_len
              # message_count = buf.byteslice(pos, 4).unpack1("L>")
              channel.message_delivered(nil, delivery_tag, redelivered == 1, exchange, routing_key)
            when 72 # get-empty
              channel.basic_get_empty
            when 80 # ack
              delivery_tag, multiple = buf.unpack("@4 Q> C")
              channel.confirm [:ack, delivery_tag, multiple == 1]
            when 111 # recover-ok
              channel.reply [:basic_recover_ok]
            when 120 # nack
              delivery_tag, multiple, requeue = buf.unpack("@4 Q> C C")
              channel.confirm [:nack, delivery_tag, multiple == 1, requeue == 1]
            else raise Error::UnsupportedMethodFrame.new class_id, method_id
            end
          when 85 # confirm
            case method_id
            when 11 # select-ok
              channel.reply [:confirm_select_ok]
            else raise Error::UnsupportedMethodFrame.new class_id, method_id
            end
          when 90 # tx
            case method_id
            when 11 # select-ok
              channel.reply [:tx_select_ok]
            when 21 # commit-ok
              channel.reply [:tx_commit_ok]
            when 31 # rollback-ok
              channel.reply [:tx_rollback_ok]
            else raise Error::UnsupportedMethodFrame.new class_id, method_id
            end
          else raise Error::UnsupportedMethodFrame.new class_id, method_id
          end
        when 2 # header
          body_size = buf.unpack1("@4 Q>")
          properties = Properties.decode(buf, 12)
          channel.header_delivered body_size, properties
        when 3 # body
          channel.body_delivered buf
        when 8 # heartbeat
          handle_server_heartbeat(channel_id)
          handle_server_heartbeat(channel_id)
        else raise Error::UnsupportedFrameType, type
        end
        true
      end

      def update_last_recv
        return unless @heartbeat&.positive?

        @last_recv = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      def update_last_sent
        return unless @heartbeat&.positive?

        @last_sent = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      def handle_server_heartbeat(channel_id)
        return if channel_id.zero?

        raise Error::ConnectionClosed.new(501, "Heartbeat frame received on non-zero channel #{channel_id}")
      end

      MAX_MISSED_HEARTBEATS = 2

      # Start the heartbeat background thread (called from connection#tune)
      def start_heartbeats(interval)
        Thread.new do
          loop do
            sleep interval / 2.0
            break if @closed
            next if @socket.nil?

            now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            # If we haven't sent anything recently, send a heartbeat
            if now - @last_sent >= interval
              begin
                send_heartbeat
              rescue Error => e
                warn "AMQP-Client heartbeat send failed: #{e.inspect}"
                break
              end
            end
            # If we haven't received anything from the server after MAX_MISSED_HEARTBEATS, close connection
            next unless now - @last_recv > interval * MAX_MISSED_HEARTBEATS

            break if @closed

            warn "AMQP-Client: closing connection due to missed server heartbeats"\
                 " (last_recv=#{@last_recv.inspect}, now=#{now.inspect}, interval=#{interval})"
            @closed = [501, "Missed server heartbeats"]
            begin
              @socket.close
            rescue StandardError => e
              warn "AMQP-Client heartbeat close failed: #{e.inspect}"
            end
            break
          end
        end
      end

      def send_heartbeat
        write_bytes FrameBytes.heartbeat
      end

      def expect(expected_frame_type)
        frame_type, args = @replies.pop
        if frame_type.nil?
          return if expected_frame_type == :close_ok

          raise Error::ConnectionClosed.new(*@closed) if @closed

          raise Error, "Connection closed while waiting for #{expected_frame_type}"
        end
        frame_type == expected_frame_type || raise(Error::UnexpectedFrame.new(expected_frame_type, frame_type))
        args
      end

      # Connect to the host/port, optionally establish a TLS connection
      # @return [Socket]
      # @return [OpenSSL::SSL::SSLSocket]
      def open_socket(host, port, tls, options)
        connect_timeout = options.fetch(:connect_timeout, 30).to_f
        socket = Socket.tcp host, port, connect_timeout: connect_timeout
        keepalive = options.fetch(:keepalive, "").split(":", 3).map!(&:to_i)
        enable_tcp_keepalive(socket, *keepalive)
        if tls
          cert_store = OpenSSL::X509::Store.new
          cert_store.set_default_paths
          context = OpenSSL::SSL::SSLContext.new
          context.cert_store = cert_store
          verify_peer = [false, "false", "none"].include? options[:verify_peer]
          context.verify_mode = OpenSSL::SSL::VERIFY_PEER unless verify_peer
          socket = OpenSSL::SSL::SSLSocket.new(socket, context)
          socket.sync_close = true # closing the TLS socket also closes the TCP socket
          socket.hostname = host # SNI host
          socket.connect
          socket.post_connection_check(host) || raise(Error, "TLS certificate hostname doesn't match requested")
        end
        socket
      rescue SystemCallError, OpenSSL::OpenSSLError => e
        raise Error, "Could not open a socket: #{e.message}"
      end

      # Negotiate a connection
      # @return [Array<Integer, Integer, Integer>] channel_max, frame_max, heartbeat
      def establish(socket, user, password, vhost, options)
        channel_max, frame_max, heartbeat = nil
        socket.write "AMQP\x00\x00\x09\x01"
        buf = String.new(capacity: 4096)
        loop do # rubocop:disable Metrics/BlockLength
          begin
            socket.readpartial(4096, buf)
          rescue *READ_EXCEPTIONS => e
            raise Error, "Could not establish AMQP connection: #{e.message}"
          end

          type, channel_id, frame_size = buf.unpack("C S> L>")
          frame_end = buf.getbyte(frame_size + 7)
          raise Error::UnexpectedFrameEnd, frame_end if frame_end != 206

          case type
          when 1 # method frame
            class_id, method_id = buf.unpack("@7 S> S>")
            case class_id
            when 10 # connection
              raise Error, "Unexpected channel id #{channel_id} for Connection frame" if channel_id != 0

              case method_id
              when 10 # connection#start
                conn_name = options[:connection_name] || $PROGRAM_NAME
                properties = CLIENT_PROPERTIES.merge({ connection_name: conn_name })
                socket.write FrameBytes.connection_start_ok "\u0000#{user}\u0000#{password}", properties
              when 30 # connection#tune
                channel_max, frame_max, heartbeat = buf.unpack("@11 S> L> S>")
                channel_max = 65_536 if channel_max.zero?
                channel_max = [channel_max, options.fetch(:channel_max, 2048).to_i].min
                frame_max = [frame_max, options.fetch(:frame_max, 131_072).to_i].min
                heartbeat = [heartbeat, options.fetch(:heartbeat, 0).to_i].min
                start_heartbeats(heartbeat) if heartbeat.positive?
                socket.write FrameBytes.connection_tune_ok(channel_max, frame_max, heartbeat)
                socket.write FrameBytes.connection_open(vhost)
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
      rescue Exception => e
        begin
          socket.close
        rescue *READ_EXCEPTIONS
          nil
        end
        raise e
      end

      # Enable TCP keepalive, which is preferred to heartbeats
      # @return [void]
      def enable_tcp_keepalive(socket, idle = 60, interval = 10, count = 3)
        socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE, true)
        if Socket.const_defined?(:TCP_KEEPIDLE) # linux/bsd
          socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_KEEPIDLE, idle)
        elsif RUBY_PLATFORM.include? "darwin" # os x
          # https://www.quickhack.net/nom/blog/2018-01-19-enable-tcp-keepalive-of-macos-and-linux-in-ruby.html
          socket.setsockopt(Socket::IPPROTO_TCP, 0x10, idle)
        else # windows
          return
        end
        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_KEEPINTVL, interval)
        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_KEEPCNT, count)
      rescue StandardError => e
        warn "AMQP-Client could not enable TCP keepalive on socket. #{e.inspect}"
      end

      # Fetch the AMQP port number from ENV
      # @return [Integer] A port number
      # @return [nil] When the environment variable AMQP_PORT isn't set
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
end
