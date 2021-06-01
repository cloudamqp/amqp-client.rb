# frozen_string_literal: true

require_relative "./message"

module AMQP
  # AMQP Channel
  class Channel
    def initialize(connection, id)
      @replies = Queue.new
      @connection = connection
      @id = id
      @consumers = {}
      @confirm = nil
      @closed = nil
    end

    attr_reader :id, :consumers

    def open
      write_bytes FrameBytes.channel_open(@id)
      expect(:channel_open_ok)
      self
    end

    def close(reason = "", code = 200)
      write_bytes FrameBytes.channel_close(@id, reason, code)
      expect :channel_close_ok
      @closed = [code, reason]
    end

    def closed!(code, reason, classid, methodid)
      write_bytes FrameBytes.channel_close_ok(@id)
      @closed = [code, reason, classid, methodid]
      @replies.close
      @consumers.each(&:close)
      @consumers.clear
    end

    def queue_declare(name = "", passive: false, durable: true, exclusive: false, auto_delete: false, **args)
      durable = false if name.empty?
      exclusive = true if name.empty?
      auto_delete = true if name.empty?

      write_bytes FrameBytes.queue_declare(@id, name, passive, durable, exclusive, auto_delete, args)
      name, message_count, consumer_count = expect(:queue_declare_ok)
      {
        queue_name: name,
        message_count: message_count,
        consumer_count: consumer_count
      }
    end

    def queue_delete(name, if_unused: false, if_empty: false, no_wait: false)
      write_bytes FrameBytes.queue_delete(@id, name, if_unused, if_empty, no_wait)
      message_count, = expect :queue_delete
      message_count
    end

    def queue_bind(name, exchange, binding_key, arguments = {})
      write_bytes FrameBytes.queue_bind(@id, name, exchange, binding_key, false, arguments)
      expect :queue_bind_ok
    end

    def queue_unbind(name, exchange, binding_key, arguments = {})
      write_bytes FrameBytes.queue_unbind(@id, name, exchange, binding_key, arguments)
      expect :queue_unbind_ok
    end

    def basic_get(queue_name, no_ack: true)
      write_bytes FrameBytes.basic_get(@id, queue_name, no_ack)
      frame, *rest = @replies.shift
      case frame
      when :basic_get_ok
        delivery_tag, exchange_name, routing_key, _message_count, redelivered = rest
        body_size, properties = expect(:header)
        pos = 0
        body = String.new("", capacity: body_size)
        while pos < body_size
          body_part, = expect(:body)
          body += body_part
          pos += body_part.bytesize
        end
        Message.new(delivery_tag, exchange_name, routing_key, properties, body, redelivered)
      when :basic_get_empty then nil
      when nil              then raise AMQP::Client::ChannelClosedError.new(@id, *@closed)
      else raise AMQP::Client::UnexpectedFrame.new(%i[basic_get_ok basic_get_empty], frame)
      end
    end

    def basic_publish(body, exchange, routing_key, mandatory: false, **properties)
      frame_max = @connection.frame_max - 8
      id = @id

      if 0 < body.bytesize && body.bytesize <= frame_max
        write_bytes FrameBytes.basic_publish(id, exchange, routing_key, mandatory),
                    FrameBytes.header(id, body.bytesize, properties),
                    FrameBytes.body(id, body)
        return @confirm ? @confirm += 1 : nil
      end

      write_bytes FrameBytes.basic_publish(id, exchange, routing_key, mandatory),
                  FrameBytes.header(id, body.bytesize, properties)
      pos = 0
      while pos < body.bytesize # split body into multiple frame_max frames
        len = [frame_max, body.bytesize - pos].min
        body_part = body.byteslice(pos, len)
        write_bytes FrameBytes.body(id, body_part)
        pos += len
      end
      @confirm += 1 if @confirm
    end

    def basic_publish_confirm(body, exchange, routing_key, mandatory, **properties)
      id = basic_publish(body, exchange, routing_key, mandatory, properties)
      wait_for_confirm(id)
    end

    def basic_consume(queue, tag: "", no_ack: true, exclusive: false, arguments: {},
                      thread_count: 1, &blk)
      write_bytes FrameBytes.basic_consume(@id, queue, tag, no_ack, exclusive, arguments)
      tag, = expect(:basic_consume_ok)
      q = @consumers[tag] = Queue.new
      msgs = Queue.new
      Thread.new { recv_deliveries(tag, q, msgs) }
      if thread_count.zero?
        while (msg = msgs.shift)
          yield msg
        end
      else
        threads = Array.new(thread_count) do
          Thread.new do
            while (msg = msgs.shift)
              blk.call(msg)
            end
          end
        end
        [tag, threads]
      end
    end

    def basic_cancel(consumer_tag, no_wait: false)
      consumer = @consumers.fetch(consumer_tag)
      return if consumer.closed?

      write_bytes FrameBytes.basic_cancel(@id, consumer_tag)
      expect(:basic_cancel_ok) unless no_wait
      consumer.close
    end

    def confirm_select(no_wait: false)
      return if @confirm

      write_bytes FrameBytes.confirm_select(@id, no_wait)
      expect :confirm_select_ok unless no_wait
      @confirms = Queue.new
      @confirm = 0
    end

    def wait_for_confirm(id)
      raise ArgumentError, "Confirm id has to a positive number" unless id&.positive?
      return true if @last_confirmed && @last_confirmed >= id

      loop do
        ack, delivery_tag, multiple = @confirms.shift || break
        @last_confirmed = delivery_tag
        return ack if delivery_tag == id || (delivery_tag > id && multiple)
      end
      false
    end

    def reply(args)
      @replies.push(args)
    end

    def confirm(args)
      @confirms.push(args)
    end

    private

    def recv_deliveries(consumer_tag, deliver_queue, msgs)
      loop do
        _, delivery_tag, redelivered, exchange, routing_key = deliver_queue.shift || raise(ClosedQueueError)
        body_size, properties = expect(:header)
        body = String.new("", capacity: body_size)
        while body.bytesize < body_size
          body_part, = expect(:body)
          body += body_part
        end
        msgs.push Message.new(delivery_tag, exchange, routing_key, properties, body,
                              redelivered, consumer_tag)
      end
    ensure
      msgs.close
    end

    def write_bytes(*bytes)
      raise AMQP::Client::ChannelClosedError.new(@id, *@closed) if @closed

      @connection.write_bytes(*bytes)
    end

    def expect(expected_frame_type)
      loop do
        frame_type, *args = @replies.shift
        raise AMQP::Client::ChannelClosedError.new(@id, *@closed) if frame_type.nil?
        return args if frame_type == expected_frame_type

        @replies.push [frame_type, *args]
      end
    end
  end
end
