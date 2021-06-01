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
      @replies = Queue.new
      Thread.new { read_loop }
    end

    attr_reader :frame_max

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
        rescue IOError, OpenSSL::OpenSSLError, SystemCallError
          break
        end

        pos = 0
        while pos < buffer.bytesize
          buffer += socket.read(pos + 8 - buffer.bytesize) if pos + 8 > buffer.bytesize
          type, channel_id, frame_size = buffer.unpack("@#{pos} C S> L>")
          if frame_size > frame_max
            raise AMQP::Client::Error, "Frame size #{frame_size} larger than negotiated max frame size #{frame_max}"
          end

          frame_end_pos = pos + 7 + frame_size
          buffer += socket.read(frame_end_pos - buffer.bytesize + 1) if frame_end_pos + 1 > buffer.bytesize
          frame_end = buffer.unpack1("@#{frame_end_pos} C")
          raise AMQP::Client::UnexpectedFrameEnd, frame_end if frame_end != 206

          buf = buffer.byteslice(pos, frame_size + 8)
          pos += frame_size + 8
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
            @replies.push [:close_ok]
            return false
          else raise AMQP::Client::UnsupportedMethodFrame, class_id, method_id
          end
        when 20 # channel
          case method_id
          when 11 # channel#open-ok
            @channels[channel_id].reply [:channel_open_ok]
          when 40 # channel#close
            reply_code, reply_text_len = buf.unpack("@11 S> C")
            reply_text, classid, methodid = buf.unpack("@14 a#{reply_text_len} S> S>")
            channel = @channels.delete(channel_id)
            channel.closed!(reply_code, reply_text, classid, methodid)
          when 41 # channel#close-ok
            @channels[channel_id].reply [:channel_close_ok]
          else raise AMQP::Client::UnsupportedMethodFrame, class_id, method_id
          end
        when 50 # queue
          case method_id
          when 11 # declare-ok
            queue_name_len = buf.unpack1("@11 C")
            queue_name, message_count, consumer_count = buf.unpack("@12 a#{queue_name_len} L> L>")
            @channels[channel_id].reply [:queue_declare_ok, queue_name, message_count, consumer_count]
          when 21 # bind-ok
            @channels[channel_id].reply [:queue_bind_ok]
          when 41 # delete-ok
            message_count = buf.unpack1("@11 L>")
            @channels[channel_id].reply [:queue_delete, message_count]
          when 51 # unbind-ok
            @channels[channel_id].reply [:queue_unbind_ok]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        when 60 # basic
          case method_id
          when 21 # consume-ok
            tag_len = buf.unpack1("@11 C")
            tag = buf.unpack1("@12 a#{tag_len}")
            @channels[channel_id].reply [:basic_consume_ok, tag]
          when 30 # cancel
            tag_len = buf.unpack1("@11 C")
            tag, no_wait = buf.unpack("@12 a#{tag_len} C")
            @channels[channel_id].consumers.fetch(tag).close
            write_bytes FrameBytes.basic_cancel_ok(@id, tag) unless no_wait == 1
          when 31 # cancel-ok
            tag_len = buf.unpack1("@11 C")
            tag = buf.unpack1("@12 a#{tag_len}")
            @channels[channel_id].reply [:basic_cancel_ok, tag]
          when 60 # deliver
            ctag_len = buf.unpack1("@11 C")
            consumer_tag, delivery_tag, redelivered, exchange_len = buf.unpack("@12 a#{ctag_len} Q> C C")
            exchange, rk_len = buf.unpack("@#{12 + ctag_len + 4 + 1 + 1} a#{exchange_len} C")
            routing_key = buf.unpack1("@#{12 + ctag_len + 4 + 1 + 1 + exchange_len + 1} a#{rk_len}")
            loop do
              if (consumer = @channels[channel_id].consumers[consumer_tag])
                consumer.push [:deliver, delivery_tag, redelivered == 1, exchange, routing_key]
                break
              else
                Thread.pass
              end
            end
          when 71 # get-ok
            delivery_tag, redelivered, exchange_len = buf.unpack("@11 Q> C C")
            exchange, routing_key_len = buf.unpack("@21 a#{exchange_len} C")
            routing_key, message_count = buf.unpack("@#{22 + exchange_len} a#{routing_key_len} L>")
            redelivered = redelivered == 1
            @channels[channel_id].reply [:basic_get_ok, delivery_tag, exchange, routing_key, message_count, redelivered]
          when 72 # get-empty
            @channels[channel_id].reply [:basic_get_empty]
          when 80 # ack
            delivery_tag, multiple = buf.unpack1("@11 Q> C")
            @channels[channel_id].confirm [:ack, delivery_tag, multiple]
          when 120 # nack
            delivery_tag, multiple, requeue = buf.unpack1("@11 Q> C C")
            @channels[channel_id].confirm [:nack, delivery_tag, multiple == 1, requeue == 1]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        when 85 # confirm
          case method_id
          when 11 # select-ok
            @channels[channel_id].reply [:confirm_select_ok]
          else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
          end
        else raise AMQP::Client::UnsupportedMethodFrame.new class_id, method_id
        end
      when 2 # header
        body_size = buf.unpack1("@11 Q>")
        @channels[channel_id].reply [:header, body_size, nil]
      when 3 # body
        body = buf.byteslice(7, frame_size)
        @channels[channel_id].reply [:body, body]
      else raise AMQP::Client::UnsupportedFrameType, type
      end
      true
    end

    def expect(expected_frame_type)
      frame_type, args = @replies.shift
      frame_type == expected_frame_type || raise(UnexpectedFrame.new(expected_frame_type, frame_type))
      args
    end
  end
end
