# frozen_string_literal: true

module AMQP
  class Client
    # Consumer abstraction
    class Consumer
      attr_reader :channel, :queue, :tag

      def initialize(channel, queue, tag)
        @channel = channel
        @queue = queue
        @tag = tag
      end

      # Cancel the consumer
      # @return [self]
      def cancel
        @channel.basic_cancel(@tag)
        self
      end
    end
  end
end
