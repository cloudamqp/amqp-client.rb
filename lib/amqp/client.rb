# frozen_string_literal: true

require "socket"
require "uri"
require "openssl"
require_relative "client/version"
require_relative "client/errors"
require_relative "client/frames"
require_relative "client/connection"
require_relative "client/channel"

module AMQP
  # AMQP 0-9-1 Client
  class Client
    def initialize(uri)
      @uri = URI.parse(uri)
      @tls = @uri.scheme == "amqps"
      @port = port_from_env || @uri.port || (@tls ? 5671 : 5672)
      @host = @uri.host || "localhost"
      @user = @uri.user || "guest"
      @password = @uri.password || "guest"
      @vhost = URI.decode_www_form_component(@uri.path[1..-1] || "/")
      @options = URI.decode_www_form(@uri.query || "").to_h
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

    private

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
              socket.write FrameBytes.connection_start_ok "\u0000#{@user}\u0000#{@password}"
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
    rescue => e
      warn "amqp-client: Could not enable TCP keepalive on socket. #{e.inspect}"
    end

    def port_from_env
      port = ENV["AMQP_PORT"]

      port.nil? ? nil : port.to_i
    end
  end
end
