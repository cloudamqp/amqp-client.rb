# frozen_string_literal: true

module AMQP
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

    def write_frame(bytes)
      @socket.write bytes
    end

    private

    def read_loop
      socket = @socket
      ibuf = String.new(capacity: 4096)
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
              @channels[channel_id].push :channel_open_ok
            when 40 # channel#close
              @channels.delete(channel_id)&.closed!
            when 41 # channel#close-ok
              @channels[channel_id].push :channel_close_ok
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
