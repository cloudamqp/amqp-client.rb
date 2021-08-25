# frozen_string_literal: true

require_relative "./message"

module AMQP
  # AMQP Channel
  class Channel
    def initialize(connection, id)
      @connection = connection
      @id = id
      @replies = ::Queue.new
      @consumers = {}
      @closed = nil
      @open = false
      @on_return = nil
      @confirm = nil
      @unconfirmed = ::Queue.new
      @unconfirmed_empty = ::Queue.new
      @return_queue = ::Queue.new
      @deliver_queue = ::Queue.new
      Thread.new { deliver_loop(@deliver_queue) }
      Thread.new { return_loop(@return_queue) }
    end

    def inspect
      "#<#{self.class} @id=#{@id} @open=#{@open} @closed=#{@closed} confirm_selected=#{!@confirm.nil?} replies_count=#{@replies.size} unconfirmed_count=#{@unconfirmed.size} consumer_count=#{@consumers.size}>"
    end

    attr_reader :id, :consumers

    def open
      return self if @open

      @open = true
      write_bytes FrameBytes.channel_open(@id)
      expect(:channel_open_ok)
      self
    end

    def close(reason = "", code = 200)
      return if @closed

      write_bytes FrameBytes.channel_close(@id, reason, code)
      @closed = [code, reason]
      expect :channel_close_ok
      @replies.close
      @unconfirmed_empty.close
      @consumers.each_value(&:close)
    end

    # Called when channel is closed by server
    def closed!(code, reason, classid, methodid)
      @closed = [code, reason, classid, methodid]
      @replies.close
      @unconfirmed_empty.close
      @consumers.each_value(&:close)
    end

    def exchange_declare(name, type, passive: false, durable: true, auto_delete: false, internal: false, arguments: {})
      write_bytes FrameBytes.exchange_declare(@id, name, type, passive, durable, auto_delete, internal, arguments)
      expect :exchange_declare_ok
    end

    def exchange_delete(name, if_unused: false, no_wait: false)
      write_bytes FrameBytes.exchange_delete(@id, name, if_unused, no_wait)
      expect :exchange_delete_ok
    end

    def exchange_bind(destination, source, binding_key, arguments: {})
      write_bytes FrameBytes.exchange_bind(@id, destination, source, binding_key, false, arguments)
      expect :exchange_bind_ok
    end

    def exchange_unbind(destination, source, binding_key, arguments: {})
      write_bytes FrameBytes.exchange_unbind(@id, destination, source, binding_key, false, arguments)
      expect :exchange_unbind_ok
    end

    QueueOk = Struct.new(:queue_name, :message_count, :consumer_count)

    def queue_declare(name = "", passive: false, durable: true, exclusive: false, auto_delete: false, arguments: {})
      durable = false if name.empty?
      exclusive = true if name.empty?
      auto_delete = true if name.empty?

      write_bytes FrameBytes.queue_declare(@id, name, passive, durable, exclusive, auto_delete, arguments)
      name, message_count, consumer_count = expect(:queue_declare_ok)

      QueueOk.new(name, message_count, consumer_count)
    end

    def queue_delete(name, if_unused: false, if_empty: false, no_wait: false)
      write_bytes FrameBytes.queue_delete(@id, name, if_unused, if_empty, no_wait)
      message_count, = expect :queue_delete
      message_count
    end

    def queue_bind(name, exchange, binding_key, arguments: {})
      write_bytes FrameBytes.queue_bind(@id, name, exchange, binding_key, false, arguments)
      expect :queue_bind_ok
    end

    def queue_purge(name, no_wait: false)
      write_bytes FrameBytes.queue_purge(@id, name, no_wait)
      expect :queue_purge_ok unless no_wait
    end

    def queue_unbind(name, exchange, binding_key, arguments: {})
      write_bytes FrameBytes.queue_unbind(@id, name, exchange, binding_key, arguments)
      expect :queue_unbind_ok
    end

    def basic_get(queue_name, no_ack: true)
      write_bytes FrameBytes.basic_get(@id, queue_name, no_ack)
      frame, *rest = @replies.pop
      case frame
      when :basic_get_ok
        delivery_tag, exchange_name, routing_key, _message_count, redelivered = rest
        body_size, properties = expect(:header)
        body = StringIO.new(String.new(capacity: body_size)).binmode
        while body.pos < body_size
          body_part, = expect(:body)
          body.write(body_part)
        end
        Message.new(self, delivery_tag, exchange_name, routing_key, properties, body.string, redelivered)
      when :basic_get_empty then nil
      when nil              then raise AMQP::Client::ChannelClosedError.new(@id, *@closed)
      else raise AMQP::Client::UnexpectedFrame.new(%i[basic_get_ok basic_get_empty], frame)
      end
    end

    def basic_publish(body, exchange, routing_key, **properties)
      frame_max = @connection.frame_max - 8
      id = @id
      mandatory = properties.delete(:mandatory) || false
      case properties.delete(:persistent)
      when true then properties[:delivery_mode] = 2
      when false then properties[:delivery_mode] = 1
      end

      if body.bytesize.between?(1, frame_max)
        write_bytes FrameBytes.basic_publish(id, exchange, routing_key, mandatory),
                    FrameBytes.header(id, body.bytesize, properties),
                    FrameBytes.body(id, body)
        @unconfirmed.push @confirm += 1 if @confirm
        return
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
      @unconfirmed.push @confirm += 1 if @confirm
      nil
    end

    def basic_publish_confirm(body, exchange, routing_key, **properties)
      confirm_select(no_wait: true)
      basic_publish(body, exchange, routing_key, **properties)
      wait_for_confirms
    end

    # Consume from a queue
    # worker_threads: 0 => blocking, messages are executed in the thread calling this method
    def basic_consume(queue, tag: "", no_ack: true, exclusive: false, arguments: {},
                      worker_threads: 1)
      write_bytes FrameBytes.basic_consume(@id, queue, tag, no_ack, exclusive, arguments)
      tag, = expect(:basic_consume_ok)
      msgs = @consumers[tag] = ::Queue.new
      if worker_threads.zero?
        while (msg = msgs.pop)
          yield msg
        end
      else
        threads = Array.new(worker_threads) do
          Thread.new do
            while (msg = msgs.pop)
              yield(msg)
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

    def basic_qos(prefetch_count, prefetch_size: 0, global: false)
      write_bytes FrameBytes.basic_qos(@id, prefetch_size, prefetch_count, global)
      expect :basic_qos_ok
    end

    def basic_ack(delivery_tag, multiple: false)
      write_bytes FrameBytes.basic_ack(@id, delivery_tag, multiple)
    end

    def basic_nack(delivery_tag, multiple: false, requeue: false)
      write_bytes FrameBytes.basic_nack(@id, delivery_tag, multiple, requeue)
    end

    def basic_reject(delivery_tag, requeue: false)
      write_bytes FrameBytes.basic_reject(@id, delivery_tag, requeue)
    end

    def basic_recover(requeue: false)
      write_bytes FrameBytes.basic_recover(@id, requeue: requeue)
      expect :basic_recover_ok
    end

    def confirm_select(no_wait: false)
      return if @confirm

      write_bytes FrameBytes.confirm_select(@id, no_wait)
      expect :confirm_select_ok unless no_wait
      @confirm = 0
    end

    # Block until all publishes messages are confirmed
    def wait_for_confirms
      return true if @unconfirmed.empty?

      case @unconfirmed_empty.pop
      when true then true
      when false then false
      else raise AMQP::Client::ChannelClosedError.new(@id, *@closed)
      end
    end

    # Called by Connection when received ack/nack from server
    def confirm(args)
      ack_or_nack, delivery_tag, multiple = *args
      loop do
        tag = @unconfirmed.pop(true)
        break if tag == delivery_tag
        next if multiple && tag < delivery_tag

        @unconfirmed << tag # requeue
      rescue ThreadError
        break
      end
      return unless @unconfirmed.empty?

      @unconfirmed_empty.num_waiting.times do
        @unconfirmed_empty << (ack_or_nack == :ack)
      end
    end

    def tx_select
      write_bytes FrameBytes.tx_select(@id)
      expect :tx_select_ok
    end

    def tx_commit
      write_bytes FrameBytes.tx_commit(@id)
      expect :tx_commit_ok
    end

    def tx_rollback
      write_bytes FrameBytes.tx_rollback(@id)
      expect :tx_rollback_ok
    end

    def reply(args)
      @replies.push(args)
    end

    def message_returned(reply_code, reply_text, exchange, routing_key)
      @return_queue.push [reply_code, reply_text, exchange, routing_key]
    end

    def message_delivered(consumer_tag, delivery_tag, redelivered, exchange, routing_key)
      @deliver_queue.push [consumer_tag, delivery_tag, redelivered, exchange, routing_key]
    end

    def on_return(&block)
      @on_return = block
    end

    private

    def deliver_loop(deliver_queue)
      consumers = @consumers
      loop do
        consumer_tag, delivery_tag, redelivered, exchange, routing_key = deliver_queue.pop || raise(ClosedQueueError)
        body_size, properties = expect(:header)
        body = StringIO.new(String.new(capacity: body_size)).binmode
        while body.pos < body_size
          body_part, = expect(:body)
          body.write(body_part)
        end
        msg = Message.new(self, delivery_tag, exchange, routing_key, properties, body.string, redelivered, consumer_tag)
        Thread.pass until (consumer = consumers[consumer_tag])
        consumer.push msg
      end
    ensure
      deliver_queue
    end

    def return_loop(return_queue)
      loop do
        reply_code, reply_text, exchange, routing_key = return_queue.pop || raise(ClosedQueueError)
        body_size, properties = expect(:header)
        body = StringIO.new(String.new(capacity: body_size)).binmode
        while body.pos < body_size
          body_part, = expect(:body)
          body.write(body_part)
        end
        msg = ReturnMessage.new(reply_code, reply_text, exchange, routing_key, properties, body.string)
        if @on_return
          @on_return.call(msg)
        else
          warn "AMQP-Client message returned: #{msg.inspect}"
        end
      end
    end

    def write_bytes(*bytes)
      raise AMQP::Client::ChannelClosedError.new(@id, *@closed) if @closed

      @connection.write_bytes(*bytes)
    end

    def expect(expected_frame_type)
      loop do
        frame_type, *args = @replies.pop
        raise AMQP::Client::ChannelClosedError.new(@id, *@closed) if frame_type.nil?
        return args if frame_type == expected_frame_type

        @replies.push [frame_type, *args] # requeue unexpected frames
      end
    end
  end
end
