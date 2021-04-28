# frozen_string_literal: true

require "socket"
require "uri"
require "openssl"
require_relative "client/version"
require_relative "client/connection"
require_relative "client/channel"

module AMQP
  # AMQP 0-9-1 Client
  class Client
    class Error < StandardError; end

    def initialize(uri)
      @uri = URI.parse(uri)
      @tls = @uri.scheme == "amqps"
      @port = @uri.port || @tls ? 5671 : 5672
      @host = @uri.host || "localhost"
      @user = @uri.user || "guest"
      @password = @uri.password || "guest"
      @vhost = URI.decode_www_form_component(@uri.path[1..-1] || "/")
    end

    def connect
      socket = TCPSocket.new @host, @port
      if @tls
        context = OpenSSL::SSL::SSLContext.new
        # context.verify_mode = OpenSSL::SSL::VERIFY_PEER
        socket = OpenSSL::SSL::SSLSocket.new(socket, context).tap do |tls_socket|
          tls_socket.sync_close = true
        end
      end
      establish(socket, @user, @password, @vhost)
      Connection.new(socket)
    end

    private

    def establish(socket, user, password, vhost)
      socket.write "AMQP\x00\x00\x09\x01"
      ibuf = String.new(capacity: 4096)
      loop do
        socket.readpartial(4096, ibuf)

        type, channel_id, frame_size = ibuf.unpack("CS>L>")
        frame_end = ibuf.byteslice(frame_size + 7).unpack1("C")
        raise AMQP::Client::Error, "Unexpected frame end" if frame_end != 206

        case type
        when 1 # method frame
          class_id, method_id = ibuf.unpack("@7S>S>")
          case class_id
          when 10 # connection
            raise AMQP::Client::Error, "Unexpected channel id #{channel_id} for Connection frame" if channel_id != 0

            case method_id
            when 10 # connection#start
              response = "\u0000#{user}\u0000#{password}"
              frame_size = 4 + 4 + 6 + 4 + response.bytesize + 1
              socket.write [
                1, # type: method
                0, # channel id
                frame_size,
                10, # class id
                11, # method id
                0, # client props
                5, "PLAIN", # mechanism
                response.bytesize, response,
                0, "", # locale
                206 # frame end
              ].pack("C S> L> S> S> L> Ca* L>a* Ca* C")
            when 30 # connection#tune
              channel_max, frame_max, heartbeat = ibuf.unpack("@11S>L>S>")

              # Connection#TuneOk
              socket.write [
                1, # type: method
                0, # channel id
                12, # frame size
                10, # class: connection
                31, # method: tune-ok
                channel_max,
                frame_max,
                heartbeat + -heartbeat,
                206 # frame end
              ].pack("CS>L>S>S>S>L>S>C")

              # Connection#Open
              frame_size = 2 + 2 + 1 + vhost.bytesize + 1 + 1
              socket.write [
                1, # type: method
                0, # channel id
                frame_size,
                10, # class: connection
                40, # method: open
                vhost.bytesize, vhost,
                0, # reserved1
                0, # reserved2
                206 # frame end
              ].pack("C S> L> S> S> Ca* CCC")
            when 41 # connection#open-ok
              return true
            else raise "Unexpected class/method: #{class_id} #{method_id}"
            end
          else raise "Unexpected class/method: #{class_id} #{method_id}"
          end
        else raise "Unexpected frame type: #{type}"
        end
      end
    end
  end
end
