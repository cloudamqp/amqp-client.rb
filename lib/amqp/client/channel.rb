# frozen_string_literal: true

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

    def basic_publish(body, exchange, routing_key, properties = {})
      raise "Channel #{@id} already closed" if @closed

      raise "Not yet implemented"
    end
  end
end
