# frozen_string_literal: true

module AMQP
  # AMQP Connection
  class Connection
    def initialize(socket, channel_max, frame_max, heartbeat)
      @socket = socket
      @channel_max = channel_max
      @frame_max = frame_max
      @heartbeat = heartbeat
      @channels = {}
      @closed = false
      @rpc = Queue.new
      Thread.new { read_loop }
    end

    def channel
      id = 1.upto(@channel_max) { |i| break i unless @channels.key? i }
      ch = Channel.new(self, id)
      @channels[id] = ch
      ch.open
    end

    def close(reason = "", code = 200)
      write_bytes FrameBytes.connection_close(code, reason)
      expect(:close_ok)
      @closed = true
    end

    def write_bytes(*bytes)
      @socket.write(*bytes)
    end

    private

    def read_loop
      socket = @socket
      frame_max = @frame_max
      buffer = String.new(capacity: frame_max)
      loop do
        begin
          socket.readpartial(frame_max, buffer)
        rescue IOError, EOFError
          break
        end

        buf_pos = 0
        while buf_pos < buffer.bytesize
          type, channel_id, frame_size = buffer.unpack("@#{buf_pos}C S> L>")
          frame_end = buffer.unpack1("@#{buf_pos + 7 + frame_size} C")
          raise AMQP::Client::UnexpectedFrameEnd if frame_end != 206

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
            write_bytes FrameBytes.connection_close_ok
            return false
          when 51 # connection#close-ok
            @rpc.push [:close_ok]
            return false
          else raise AMQP::Client::UnsupportedMethodFrame, class_id, method_id
          end
        when 20 # channel
          case method_id
          when 11 # channel#open-ok
            @channels[channel_id].push [:channel_open_ok]
          when 40 # channel#close
            channel = @channels.delete(channel_id)
            channel&.closed!
          when 41 # channel#close-ok
            @channels[channel_id].push [:channel_close_ok]
          else raise AMQP::Client::UnsupportedMethodFrame, class_id, method_id
          end
        when 50 # queue
          case method_id
          when 11 # queue#declare-ok
            queue_name_len = buf.unpack1("@11 C")
            queue_name, message_count, consumer_count = buf.unpack("@12 a#{queue_name_len} L> L>")
            @channels[channel_id].push [:queue_declare_ok, queue_name, message_count, consumer_count]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        when 60 # basic
          case method_id
          when 21 # consume-ok
            tag_len = buf.unpack1("@11 C")
            tag = buf.unpack1("@12 a#{tag_len}")
            @channels[channel_id].push [:basic_consume_ok, tag]
          when 31 # cancel-ok
            tag_len = buf.unpack1("@11 C")
            tag = buf.unpack1("@12 a#{tag_len}")
            consumer = @channels[channel_id].consumers.delete(tag)
            consumer.close
          when 60 # deliver
            ctag_len = buf.unpack1("@11 C")
            consumer_tag, delivery_tag, redelivered, exchange_len = buf.unpack("@12 a#{ctag_len} L> C C")
            exchange, rk_len = buf.unpack("@#{12 + ctag_len + 4 + 1 + 1} a#{exchange_len} C")
            routing_key = buf.unpack1("@#{12 + ctag_len + 4 + 1 + 1 + exchange_len + 1} a#{rk_len}")
            @channels[channel_id].consumers[consumer_tag].push [:deliver, delivery_tag, redelivered == 1, exchange, routing_key]
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
            @channels[channel_id].push [:basic_get_empty]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
        end
      when 2 # header
        body_size = buf.unpack1("@11 Q>")
        @channels[channel_id].push [:header, body_size, nil]
      when 3 # body
        body = buf.byteslice(7, frame_size)
        @channels[channel_id].push [:body, body]
      else raise AMQP::Client::UnsupportedFrameType, type
      end
      true
    end

    def expect(expected_frame_type)
      frame_type, args = @rpc.shift
      frame_type == expected_frame_type || raise(UnexpectedFrame.new(expected_frame_type, frame_type))
      args
    end
  end
end
