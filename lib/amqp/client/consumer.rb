# frozen_string_literal: true

module AMQP
  class Client
    # Consumer abstraction
    class Consumer
      attr_reader :queue, :id, :channel_id, :tag, :no_ack, :prefetch, :worker_threads, :arguments, :block

      # @api private
      def initialize(client:, channel_id:, id:, consume_args:, consume_ok:)
        @client = client
        @channel_id = channel_id
        @id = id
        @queue = consume_args.queue
        @no_ack = consume_args.no_ack
        @prefetch = consume_args.prefetch
        @worker_threads = consume_args.worker_threads
        @arguments = consume_args.arguments
        @block = consume_args.block
        @tag = consume_ok.consumer_tag
      end

      # Update the consumer with new channel and tag after reconnection
      # @api private
      def update_channel_id_and_tag(channel_id, tag)
        @channel_id = channel_id
        @tag = tag
      end

      # Cancel the consumer
      # @return [self]
      def cancel
        @client.cancel_consumer(self)
        self
      end
    end
  end
end
