# frozen_string_literal: true

require_relative "consumer"

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
      # @param body [String] The body
      # @option (see Client#publish)
      # @raise (see Client#publish)
      # @return [self]
      def publish(body, **properties)
        @client.publish(body, exchange: "", routing_key: @name, **properties)
        self
      end

      # Publish to the queue, without waiting for confirm
      # @param (see Client#publish_and_forget)
      # @option (see Client#publish_and_forget)
      # @return [self]
      def publish_and_forget(body, **properties)
        @client.publish_and_forget(body, "", @name, **properties)
        self
      end

      # Subscribe/consume from the queue
      # @param no_ack [Boolean] If true, messages are automatically acknowledged by the server upon delivery.
      #   If false, messages are acknowledged only after the block completes successfully; if the block raises
      #   an exception, the message is rejected and can be optionally requeued. (Default: false)
      # @param prefetch [Integer] Specify how many messages to prefetch for consumers with no_ack is false
      # @param worker_threads [Integer] Number of threads processing messages,
      #   0 means that the thread calling this method will be blocked
      # @param requeue_on_reject [Boolean] If true, messages that are rejected due to an exception in the block
      #   will be requeued. Only relevant if no_ack is false. (Default: true)
      # @param arguments [Hash] Custom arguments to the consumer
      # @yield [Message] Delivered message from the queue
      # @return [Consumer] The consumer object, which can be used to cancel the consumer
      def subscribe(no_ack: false, prefetch: 1, worker_threads: 1, requeue_on_reject: true, arguments: {})
        @client.subscribe(@name, no_ack:, prefetch:, worker_threads:, arguments:) do |message|
          yield message
          message.ack unless no_ack
        rescue StandardError => e
          message.reject(requeue: requeue_on_reject) unless no_ack
          raise e
        end
      end

      # Bind the queue to an exchange
      # @param exchange [String, Exchange] Name of the exchange to bind to, or the exchange object itself
      # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
      # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
      # @return [self]
      def bind(exchange, binding_key: "", arguments: {})
        exchange = exchange.name unless exchange.is_a?(String)
        @client.bind(queue: @name, exchange:, binding_key:, arguments:)
        self
      end

      # Unbind the queue from an exchange
      # @param exchange [String, Exchange] Name of the exchange to unbind from, or the exchange object itself
      # @param binding_key [String] Binding key which the queue is bound to the exchange with
      # @param arguments [Hash] Arguments matching the binding that's being removed
      # @return [self]
      def unbind(exchange, binding_key: "", arguments: {})
        exchange = exchange.name unless exchange.is_a?(String)
        @client.unbind(queue: @name, exchange:, binding_key:, arguments:)
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
