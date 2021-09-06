# frozen_string_literal: true

module AMQP
  class Client
    # Queue abstraction
    class Queue
      # Should only be initialized from the Client
      # @api private
      def initialize(client, name)
        @client = client
        @name = name
      end

      # Publish to the queue
      # @param body [String] The message body
      # @param properties [Properties]
      # @option properties [String] content_type Content type of the message body
      # @option properties [String] content_encoding Content encoding of the body
      # @option properties [Hash<String, Object>] headers Custom headers
      # @option properties [Integer] delivery_mode 2 for persisted message, transient messages for all other values
      # @option properties [Integer] priority A priority of the message (between 0 and 255)
      # @option properties [Integer] correlation_id A correlation id, most often used used for RPC communication
      # @option properties [String] reply_to Queue to reply RPC responses to
      # @option properties [Integer, String] expiration Number of seconds the message will stay in the queue
      # @option properties [String] message_id Can be used to uniquely identify the message, e.g. for deduplication
      # @option properties [Date] timestamp Often used for the time the message was originally generated
      # @option properties [String] type Can indicate what kind of message this is
      # @option properties [String] user_id Can be used to verify that this is the user that published the message
      # @option properties [String] app_id Can be used to indicates which app that generated the message
      # @return [Queue] self
      def publish(body, **properties)
        @client.publish(body, "", @name, **properties)
        self
      end

      # Subscribe/consume from the queue
      # @return [Queue] self
      def subscribe(no_ack: false, prefetch: 1, worker_threads: 1, arguments: {}, &blk)
        @client.subscribe(@name, no_ack: no_ack, prefetch: prefetch, worker_threads: worker_threads, arguments: arguments, &blk)
        self
      end

      # Bind the queue to an exchange
      # @param exchange [String] Name of the exchange to bind to
      # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
      # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
      # @return [Queue] self
      def bind(exchange, binding_key, arguments: {})
        @client.bind(@name, exchange, binding_key, arguments: arguments)
        self
      end

      # Unbind the queue from an exchange
      # @param exchange [String] Name of the exchange to unbind from
      # @param binding_key [String] Binding key which the queue is bound to the exchange with
      # @param arguments [Hash] Arguments matching the binding that's being removed
      # @return [Queue] self
      def unbind(exchange, binding_key, arguments: {})
        @client.unbind(@name, exchange, binding_key, arguments: arguments)
        self
      end

      # Purge/empty the queue
      # @return [Queue] self
      def purge
        @client.purge(@name)
        self
      end

      # Delete the queue
      # @return [nil]
      def delete
        @client.delete_queue(@name)
        nil
      end
    end
  end
end
