# frozen_string_literal: true

module AMQP
  class Client
    # Queue abstraction
    class Queue
      def initialize(client, name)
        @client = client
        @name = name
      end

      # Publish to the queue
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
      # @return [Queue] self
      def bind(exchange, routing_key, **headers)
        @client.bind(@name, exchange, routing_key, **headers)
        self
      end

      # Unbind the queue from an exchange
      # @return [Queue] self
      def unbind(exchange, routing_key, **headers)
        @client.unbind(@name, exchange, routing_key, **headers)
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
