# frozen_string_literal: true

require_relative "./message"

module AMQP
  # AMQP Channel
  class Channel
    def initialize(connection, id)
      @rpc = Queue.new
      @connection = connection
      @id = id
      @closed = false
    end

    attr_reader :id

    def open
      write_bytes FrameBytes.channel_open(@id)
      expect(:channel_open_ok)
      self
    end

    def close(reason = "", code = 200)
      return if @closed

      write_bytes FrameBytes.channel_close(@id, reason, code)
      expect :channel_close_ok
      @closed = true
    end

    def queue_declare(name = "", passive: false, durable: true, exclusive: false, auto_delete: false, **args)
      durable = false if name.empty?
      exclusive = true if name.empty?
      auto_delete = true if name.empty?

      write_bytes FrameBytes.queue_declare(@id, name, passive, durable, exclusive, auto_delete)
      name, message_count, consumer_count = expect(:queue_declare_ok)
      {
        queue_name: name,
        message_count: message_count,
        consumer_count: consumer_count
      }
    end

    def basic_get(queue_name, no_ack: true)
      return if @closed

      write_bytes FrameBytes.basic_get(@id, queue_name, no_ack)
      resp = @rpc.shift
      frame, = resp
      case frame
      when :basic_get_ok
        _, exchange_name, routing_key, redelivered = resp
        body_size, properties = expect(:header)
        pos = 0
        body = ""
        while pos < body_size
          body_part = expect(:body)
          body += body_part
          pos += body_part.bytesize
        end
        Message.new(exchange_name, routing_key, properties, body, redelivered)
      when :basic_get_empty
        nil
      else raise AMQP::Client::UnexpectedFrame, %i[basic_get_ok basic_get_empty], frame
      end
    end

    def basic_publish(exchange, routing_key, body, properties = {})
      raise AMQP::Client::ChannelClosedError, @id if @closed

      write_bytes FrameBytes.basic_publish(@id, exchange, routing_key)
      write_bytes FrameBytes.header(@id, body.bytesize, properties)

      # body frames, splitted on frame size
      pos = 0
      while pos < body.bytesize
        len = [4096, body.bytesize - pos].min
        body_part = body.byteslice(pos, len)
        write_bytes FrameBytes.body(@id, body_part)
        pos += len
      end
    end

    def push(*args)
      @rpc.push(*args)
    end

    private

    def write_bytes(bytes)
      @connection.write_bytes bytes
    end

    def expect(expected_frame_type)
      frame_type, args = @rpc.shift
      frame_type == expected_frame_type || raise(UnexpectedFrame, expected_frame_type, frame_type)
      args
    end
  end
end
