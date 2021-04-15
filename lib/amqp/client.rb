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
              frame_size = 4 + 6 + response.bytesize + 1 + 1
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

  # AMQP Channel
  class Channel
    def initialize(connection, id)
      @rpc = Queue.new
      @connection = connection
      @id = id
    end

    def open
      @socket.write [
        1, # type: method
        id, # channel id
        5, # frame size
        20, # class: channel
        10, # method: open
        0, # reserved1
        206 # frame end
      ].pack("C S> L> S> S> C C")
      frame, frame_id = @rpc.shift
      frame == :channel_open_ok || raise("Unexpected frame #{frame}")
      frame_id == id || raise("Unexpected frame id #{frame_id}")
    end

    def push(*args)
      @rpc.push(args)
    end

    def basic_publish(body, exchange, routing_key, properties = {})
      raise "Not yet implemented"
    end
  end

  # AMQP Connection
  class Connection
    def initialize(socket)
      @channels = {}
      @socket = socket
      @closed = false
      @rpc = Queue.new
      Thread.new { read_loop }
    end

    def channel
      id = 1.upto(2048) { |i| break i unless @channels.key? i }
      ch = Channel.new(self, id)
      @channels[id] = ch
      ch.open
    end

    def close(reason = "", code = 200)
      frame_size = 2 + 2 + 2 + 1 + reason.bytesize + 2 + 2
      @socket.write [
        1, # type: method
        0, # channel id
        frame_size, # frame size
        10, # class: connection
        50, # method: close
        code,
        reason.bytesize, reason,
        0, # error class id
        0, # error method id
        206 # frame end
      ].pack("C S> L> S> S> S> Ca* S> S> C")
      @rpc.shift == :close_ok || raise("Unexpected frame")
    end

    private

    def read_loop
      socket = @socket
      ibuf = String.new(capacity: 4096)
      obuf = String.new(capacity: 4096)
      loop do
        begin
          socket.readpartial(4096, ibuf)
        rescue IOError, EOFError
          break
        end

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
            when 50 # connection#close
              code, text_len = ibuf.unpack("@11 S> C")
              text, error_class_id, error_method_id = ibuf.unpack("@14 a#{text_len} S> S>")
              warn "Connection closed #{code} #{text} #{error_class_id} #{error_method_id}"
              socket.write [
                1, # type: method
                0, # channel id
                4, # frame size
                10, # class: connection
                51, # method: close-ok
                206 # frame end
              ].pack("C S> L> S> S> C")
              socket.close
              @closed = true
              return
            when 51 # connection#close-ok
              socket.close
              @closed = true
              @rpc.push :close_ok
              return
            else raise "Unsupported class/method: #{class_id} #{method_id}"
            end
          when 20 # channel
            case method_id
            when 11 # channel#open-ok
              @channels[channel_id].push [:channel_open_ok, channel_id]
            when 40 # channel#close
              @channels.delete(channel_id)&.closed!
            when 41 # channel#close-ok
              @channels.delete(channel_id)&.closed!
            else raise "Unsupported class/method: #{class_id} #{method_id}"
            end
          when 60 # basic
            case method_id
            when 99
              # noop
            else raise "Unsupported class/method: #{class_id} #{method_id}"
            end
          else raise "Unsupported class/method: #{class_id} #{method_id}"
          end
        else raise "Unsupported frame type: #{type}"
        end
      end
    end
  end
end
