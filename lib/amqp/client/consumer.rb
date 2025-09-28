# frozen_string_literal: true

module AMQP
  class Client
    # Consumer abstraction
    class Consumer
      attr_reader :queue, :id, :channel_id, :prefetch, :block, :basic_consume_args

      # @api private
      def initialize(client:, channel_id:, id:, block:, **settings)
        @client = client
        @channel_id = channel_id
        @id = id
        @queue = settings.fetch(:queue)
        @basic_consume_args = settings.fetch(:basic_consume_args)
        @prefetch = settings.fetch(:prefetch)
        @consume_ok = settings.fetch(:consume_ok)
        @block = block
      end

      # Cancel the consumer
      # @return [self]
      def cancel
        @client.cancel_consumer(self)
        self
      end

      # True if the consumer is cancelled/closed
      # @return [Boolean]
      def closed?
        @consume_ok.msg_q.closed?
      end

      # Return the consumer tag
      # @return [String]
      def tag
        @consume_ok.consumer_tag
      end

      # Update the consumer with new metadata after reconnection
      # @api private
      def update_consume_ok(consume_ok)
        @consume_ok = consume_ok
      end
    end
  end
end
