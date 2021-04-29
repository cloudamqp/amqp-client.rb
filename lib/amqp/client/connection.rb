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
      buffer = String.new(capacity: 4096)
      loop do
        begin
          socket.readpartial(4096, buffer)
        rescue IOError, EOFError
          break
        end

        buf_pos = 0
        while buf_pos < buffer.bytesize
          type, channel_id, frame_size = buffer.unpack("@#{buf_pos}C S> L>")
          frame_end = buffer.unpack1("@#{buf_pos + 7 + frame_size} C")
          raise AMQP::Client::Error, "Unexpected frame end" if frame_end != 206

          buf = buffer.byteslice(buf_pos, frame_size + 8)
          buf_pos += frame_size + 8
          parse_frame(type, channel_id, frame_size, buf) || return
        end
      end
    ensure
      @closed = true
      begin
        @socket.close
      rescue IOError
        nil
      end
    end

    def parse_frame(type, channel_id, frame_size, buf)
      case type
      when 1 # method frame
        class_id, method_id = buf.unpack("@7 S> S>")
        case class_id
        when 10 # connection
          raise AMQP::Client::Error, "Unexpected channel id #{channel_id} for Connection frame" if channel_id != 0

          case method_id
          when 50 # connection#close
            code, text_len = buf.unpack("@11 S> C")
            text, error_class_id, error_method_id = buf.unpack("@14 a#{text_len} S> S>")
            warn "Connection closed #{code} #{text} #{error_class_id} #{error_method_id}"
            socket.write [
              1, # type: method
              0, # channel id
              4, # frame size
              10, # class: connection
              51, # method: close-ok
              206 # frame end
            ].pack("C S> L> S> S> C")
            return false
          when 51 # connection#close-ok
            @rpc.push :close_ok
            return false
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
        when 50 # queue
          case method_id
          when 11 # queue#declare-ok
            queue_name_len = buf.unpack1("@11 C")
            queue_name, message_count, consumer_count = buf.unpack("@12 a#{queue_name_len} L> L>")
            @channels[channel_id].push [:queue_declare_ok, queue_name, message_count, consumer_count]
          else raise "Unsupported class/method: #{class_id} #{method_id}"
          end
        when 60 # basic
          case method_id
          when 71 # get-ok
            delivery_tag, redelivered, exchange_name_len = buf.unpack("@11 Q> C C")
            exchange_name = buf.byteslice(21, exchange_name_len)
            pos = 21 + exchange_name_len
            routing_key_len = buf.unpack1("@#{pos} C")
            pos += 1
            routing_key = buf.byteslice(pos, routing_key_len)
            pos += routing_key_len
            message_count = buf.unpack1("@#{pos} L>")
            @channels[channel_id].push [:basic_get_ok, delivery_tag, exchange_name, routing_key, message_count, redelivered == 1]
          when 72 # get-empty
            @channels[channel_id].push :basic_get_empty
          else raise "Unsupported class/method: #{class_id} #{method_id}"
          end
        else raise "Unsupported class/method: #{class_id} #{method_id}"
        end
      when 2 # header
        body_size = buf.unpack1("@11 Q>")
        @channels[channel_id].push [:header, body_size, nil]
      when 3 # body
        body = buf.byteslice(7, frame_size)
        @channels[channel_id].push [:body, body]
      else raise "Unsupported frame type: #{type}"
      end
      true
    end
  end
end
