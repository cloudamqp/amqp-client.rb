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
      # @option options [Integer] connect_timeout (30) TCP connection timeout
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
        @closed = nil
        @replies = ::Queue.new
        @write_lock = Mutex.new
        @blocked = nil
        @on_blocked = ->(reason) { warn "AMQP-Client blocked by broker: #{reason}" }
        @on_unblocked = -> { warn "AMQP-Client unblocked by broker" }

        Thread.new { read_loop } if read_loop_thread
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

        if id
          ch = @channels[id] ||= Channel.new(self, id)
        else
          1.upto(@channel_max) do |i|
            break id = i unless @channels.key? i
          end
          raise Error, "Max channels reached" if id.nil?

          ch = @channels[id] = Channel.new(self, id)
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
          if RUBY_ENGINE == "truffleruby"
            bytes.each { |b| @socket.write b }
          else
            @socket.write(*bytes)
          end
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
          raise UnexpectedFrameEnd, frame_end if frame_end != 206

          # parse the frame, will return false if a close frame was received
          parse_frame(type, channel_id, frame_buffer) || return
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

      def parse_frame(type, channel_id, buf) # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
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
              @write_lock.lock
              @on_blocked.call(reason)
            when 61 # connection#unblocked
              @write_lock.unlock
              @blocked = nil
              @on_unblocked.call
            else raise Error::UnsupportedMethodFrame, class_id, method_id
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
              channel.closed!(:channel, reply_code, reply_text, classid, methodid)
              write_bytes FrameBytes.channel_close_ok(channel_id)
            when 41 # channel#close-ok
              channel = @channels.delete(channel_id)
              channel.reply [:channel_close_ok]
            else raise Error::UnsupportedMethodFrame, class_id, method_id
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
            else raise Error::UnsupportedMethodFrame, class_id, method_id
            end
          when 50 # queue
            case method_id
            when 11 # declare-ok
              queue_name_len = buf.getbyte(4)
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
            else raise Error::UnsupportedMethodFrame.new class_id, method_id
            end
          when 60 # basic
            case method_id
            when 11 # qos-ok
              @channels[channel_id].reply [:basic_qos_ok]
            when 21 # consume-ok
              tag_len = buf.getbyte(4)
              tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
              @channels[channel_id].reply [:basic_consume_ok, tag]
            when 30 # cancel
              tag_len = buf.getbyte(4)
              tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
              no_wait = buf.getbyte(5 + tag_len) == 1
              @channels[channel_id].close_consumer(tag)
              write_bytes FrameBytes.basic_cancel_ok(@id, tag) unless no_wait
            when 31 # cancel-ok
              tag_len = buf.getbyte(4)
              tag = buf.byteslice(5, tag_len).force_encoding("utf-8")
              @channels[channel_id].reply [:basic_cancel_ok, tag]
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
              @channels[channel_id].message_returned(reply_code, reply_text, exchange, routing_key)
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
              @channels[channel_id].message_delivered(consumer_tag, delivery_tag, redelivered == 1, exchange, routing_key)
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
            else raise Error::UnsupportedMethodFrame.new class_id, method_id
            end
          when 85 # confirm
            case method_id
            when 11 # select-ok
              @channels[channel_id].reply [:confirm_select_ok]
            else raise Error::UnsupportedMethodFrame.new class_id, method_id
            end
          when 90 # tx
            case method_id
            when 11 # select-ok
              @channels[channel_id].reply [:tx_select_ok]
            when 21 # commit-ok
              @channels[channel_id].reply [:tx_commit_ok]
            when 31 # rollback-ok
              @channels[channel_id].reply [:tx_rollback_ok]
            else raise Error::UnsupportedMethodFrame.new class_id, method_id
            end
          else raise Error::UnsupportedMethodFrame.new class_id, method_id
          end
        when 2 # header
          body_size = buf.unpack1("@4 Q>")
          properties = Properties.decode(buf, 12)
          @channels[channel_id].header_delivered body_size, properties
        when 3 # body
          @channels[channel_id].body_delivered buf
        else raise Error::UnsupportedFrameType, type
        end
        true
      end

      def expect(expected_frame_type)
        frame_type, args = @replies.pop
        if frame_type.nil?
          return if expected_frame_type == :close_ok

          raise(Error::ConnectionClosed, "while waiting for #{expected_frame_type}")
        end
        frame_type == expected_frame_type || raise(Error::UnexpectedFrame.new(expected_frame_type, frame_type))
        args
      end

      # Connect to the host/port, optionally establish a TLS connection
      # @return [Socket]
      # @return [OpenSSL::SSL::SSLSocket]
      def open_socket(host, port, tls, options)
        connect_timeout = options.fetch(:connect_timeout, 30).to_i
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
        loop do
          begin
            socket.readpartial(4096, buf)
          rescue *READ_EXCEPTIONS => e
            raise Error, "Could not establish AMQP connection: #{e.message}"
          end

          type, channel_id, frame_size = buf.unpack("C S> L>")
          frame_end = buf.getbyte(frame_size + 7)
          raise UnexpectedFrameEndError, frame_end if frame_end != 206

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

      # Enable TCP keepalive, which is prefered to heartbeats
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
