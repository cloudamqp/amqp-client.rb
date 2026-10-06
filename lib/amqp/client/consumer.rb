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
        @wait_for_exclusive_use = settings.fetch(:wait_for_exclusive_use)
        @block = block
        @closed = false
      end

      # Cancel the consumer
      # @return [self]
      def cancel
        @closed = true
        @client.cancel_consumer(self)
        self
      end

      # True if the consumer is cancelled/closed
      # @return [Boolean]
      def closed?
        @closed || (!waiting? && @consume_ok.msg_q.closed?)
      end

      # True if the broker is delivering messages to the consumer. False while it waits
      # for its queue to stop being in exclusive use, or for a reconnect.
      # @return [Boolean]
      def active?
        !waiting? && !@consume_ok.msg_q.closed?
      end

      # Return the consumer tag
      # @return [String, nil] nil while waiting for a queue in exclusive use
      def tag
        @consume_ok&.consumer_tag
      end

      # Update the consumer with new metadata after reconnection
      # @api private
      def update_consume_ok(consume_ok, channel_id)
        @consume_ok = consume_ok
        @channel_id = channel_id
      end

      # True if subscribed with `exclusive: :wait`, to wait while its queue is in exclusive use
      # @api private
      def wait_for_exclusive_use?
        @wait_for_exclusive_use
      end

      # Mark the consumer as waiting for its queue to stop being in exclusive use
      # @api private
      def wait_for_queue
        update_consume_ok(nil, nil)
      end

      # Mark the consumer as closed without telling the broker, as it isn't subscribed
      # @api private
      def closed!
        @closed = true
      end

      private

      def waiting?
        @consume_ok.nil?
      end
    end
  end
end
