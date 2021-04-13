# frozen_string_literal: true

require "socket"
require "uri"
require "openssl"
require_relative "client/version"

module AMQP
  # AMQP 0-9-1 Client
  class Client
    class Error < StandardError; end

    def initialize(uri)
      @uri = URI.parse(uri)
    end

    def connect
      tls = @uri.scheme == "amqps"
      port = @uri.port || tls ? 5671 : 5672
      host = @uri.host || "localhost"
      tcp_socket = TCPSocket.new host, port
      socket =
        if tls
          context = OpenSSL::SSL::SSLContext.new
          # context.verify_mode = OpenSSL::SSL::VERIFY_PEER
          OpenSSL::SSL::SSLSocket.new(tcp_socket, context).tap do |tls_socket|
            tls_socket.sync_close = true
          end
        else
          tcp_socket
        end
      Connection.new(socket)
    end
  end

  # AMQP Connection
  class Connection
    def initialize(socket)
      @socket = socket
      @socket.write "AMQP\x00\x00\x09\x01"
      Thread.new(socket) { |sock| read_loop(sock) }
    end

    private

    def read_loop(socket)
      buf = String.new(capacity: 4096)
      loop do
        socket.readpartial(4096, buf)

        type, channel_id, frame_size = buf[0..5].unpack("CS>L>")
        case type
        when 1 # method frame
          class_id, method_id = buf[5..8].unpack("S>S>")
          case class_id
          when 10 # connection
            case method_id
            when 10 # connection#start
              # skip
              out = String.new(capacity: 4096)
              [
                1, # type: method
                0, # channel id
                0, # frame size
                10, # class id
                11, # method id
                0, # client props
                5, "PLAIN", # mechanism
                "\u0000#{user}\u0000#{password}", # response
                0, "", # locale
                "\xCE" # frame end
              ].pack("CS>L>S>S>L>Ca*L>a*Ca*C", buffer: out)
              socket.write out
            end
          end
        end
        raise Error, "Unexpected frame end" unless buf[frame_size - 1] == "\xCE"
      end
    end
  end
end

AMQP::Client.new("amqp://localhost").connect
