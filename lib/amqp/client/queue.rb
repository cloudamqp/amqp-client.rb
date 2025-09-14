# frozen_string_literal: true

module AMQP
  class Client
    # Queue abstraction
    class Queue
      attr_reader :name

      # Should only be initialized from the Client
      # @api private
      def initialize(client, name)
        @client = client
        @name = name
      end

      # Publish to the queue, wait for confirm
      # @param (see Client#publish)
      # @option (see Client#publish)
      # @raise (see Client#publish)
      # @return [self]
      def publish(body, **properties)
        @client.publish(body, "", @name, **properties)
        self
      end

      # Subscribe/consume from the queue
      # @param no_ack [Boolean] When false messages have to be manually acknowledged (or rejected)
      # @param prefetch [Integer] Specify how many messages to prefetch for consumers with no_ack is false
      # @param worker_threads [Integer] Number of threads processing messages,
      #   0 means that the thread calling this method will be blocked
      # @param arguments [Hash] Custom arguments to the consumer
      # @yield [Message] Delivered message from the queue
      # @return [self]
      def subscribe(no_ack: false, prefetch: 1, worker_threads: 1, arguments: {}, &blk)
        @client.subscribe(@name, no_ack: no_ack, prefetch: prefetch, worker_threads: worker_threads, arguments: arguments, &blk)
        self
      end

      # Bind the queue to an exchange
      # @param exchange [String | Exchange] Name of the exchange to bind to, or the exchange object itself
      # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
      # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
      # @return [self]
      def bind(exchange, binding_key, arguments: {})
        exchange = exchange.is_a?(String) ? exchange : exchange.name
        @client.bind(@name, exchange, binding_key, arguments: arguments)
        self
      end

      # Unbind the queue from an exchange
      # @param exchange [String | Exchange] Name of the exchange to unbind from, or the exchange object itself
      # @param binding_key [String] Binding key which the queue is bound to the exchange with
      # @param arguments [Hash] Arguments matching the binding that's being removed
      # @return [self]
      def unbind(exchange, binding_key, arguments: {})
        exchange = exchange.is_a?(String) ? exchange : exchange.name
        @client.unbind(@name, exchange, binding_key, arguments: arguments)
        self
      end

      # Purge/empty the queue
      # @return [self]
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
