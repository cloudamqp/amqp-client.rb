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

    def open
      @connection.write_frame [
        1, # type: method
        @id, # channel id
        5, # frame size
        20, # class: channel
        10, # method: open
        0, # reserved1
        206 # frame end
      ].pack("C S> L> S> S> C C")
      frame = @rpc.shift
      frame == :channel_open_ok || raise("Unexpected frame #{frame}")
      self
    end

    def push(*args)
      @rpc.push(*args)
    end

    def close(reason = "", code = 200)
      return if @closed

      frame_size = 2 + 2 + 2 + 1 + reason.bytesize + 2 + 2
      @connection.write_frame [
        1, # type: method
        @id, # channel id
        frame_size, # frame size
        20, # class: channel
        40, # method: close
        code,
        reason.bytesize, reason,
        0, # error class id
        0, # error method id
        206 # frame end
      ].pack("C S> L> S> S> S> Ca* S> S> C")
      @rpc.shift == :channel_close_ok || raise("Unexpected frame #{frame}")
      @closed = true
    end

    def queue_declare(name, passive: false, durable: true, exclusive: false, auto_delete: false, **args)
      durable = false if name.empty?
      exclusive = true if name.empty?
      auto_delete = true if name.empty?
      no_wait = false

      bits = 0
      bits |= (1 << 0) if passive
      bits |= (1 << 1) if durable
      bits |= (1 << 2) if exclusive
      bits |= (1 << 3) if auto_delete
      bits |= (1 << 4) if no_wait
      frame_size = 2 + 2 + 2 + 1 + name.bytesize + 1 + 4
      @connection.write_frame [
        1, # type: method
        @id, # channel id
        frame_size, # frame size
        50, # class: queue
        10, # method: declare
        0, # reserved1
        name.bytesize, name,
        bits,
        0, # arguments
        206 # frame end
      ].pack("C S> L> S> S> S> Ca* C L> C")
      frame, name, message_count, consumer_count = @rpc.shift
      raise "unexpected frame" if frame != :queue_declare_ok

      {
        queue_name: name,
        message_count: message_count,
        consumer_count: consumer_count
      }
    end

    def basic_get(queue_name, no_ack: true)
      return if @closed

      frame_size = 2 + 2 + 2 + 1 + queue_name.bytesize + 2 + 2
      @connection.write_frame [
        1, # type: method
        @id, # channel id
        frame_size, # frame size
        60, # class: basic
        70, # method: get
        0, # reserved1
        queue_name.bytesize, queue_name,
        no_ack ? 1 : 0,
        206 # frame end
      ].pack("C S> L> S> S> S> Ca* C C")
      resp = @rpc.shift
      frame, = resp
      case frame
      when :basic_get_ok
        _, exchange_name, routing_key, redelivered = resp
        frame, body_size, properties = @rpc.shift
        raise "unexpected frame #{frame}" if frame != :header

        pos = 0
        body = ""
        while pos < body_size
          frame, body_part = @rpc.shift
          raise "unexpected frame #{frame}" if frame != :body

          body += body_part
          pos += body_part.bytesize
        end
        Message.new(exchange_name, routing_key, properties, body, redelivered)
      when :basic_get_empty
        nil
      else raise("Unexpected frame #{frame}")
      end
    end


    def basic_publish(exchange, routing_key, body, properties = {})
      raise "Channel #{@id} already closed" if @closed

      frame_size = 2 + 2 + 2 + 1 + exchange.bytesize + 1 + routing_key.bytesize + 1
      @connection.write_frame [
        1, # type: method
        @id, # channel id
        frame_size, # frame size
        60, # class: basic
        40, # method: publish
        0, # reserved1
        exchange.bytesize, exchange,
        routing_key.bytesize, routing_key,
        0, # bits, mandatory/immediate
        206 # frame end
      ].pack("C S> L> S> S> S> Ca* Ca* C C")

      # headers
      frame_size = 2 + 2 + 8 + 2
      @connection.write_frame [
        2, # type: header
        @id, # channel id
        frame_size, # frame size
        60, # class: basic
        0, # weight
        body.bytesize,
        0, # properties
        206 # frame end
      ].pack("C S> L> S> S> Q> S> C")

      # body frames, splitted on frame size
      pos = 0
      while pos < body.bytesize
        len = [4096, body.bytesize - pos].min
        body_part = body.byteslice(pos, len)
        @connection.write_frame [
          3, # type: body
          @id, # channel id
          len, # frame size
          body_part,
          206 # frame end
        ].pack("C S> L> a* C")
        pos += len
      end
    end
  end
end
