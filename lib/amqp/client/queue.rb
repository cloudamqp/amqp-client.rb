# frozen_string_literal: true

require_relative "consumer"

module AMQP
  class Client
    # Queue abstraction
    class Queue
      attr_reader :name

      # Should only be initialized from the Client
      # Internal API.
      def initialize(client, name)
        @client = client
        @name = name
      end

      # Publish to the queue, wait for confirm
      # * <tt>body</tt> (<tt>Object</tt>) - The message body
      #   will be encoded if any matching codec is found in the client's codec registry
      # Options are the same as Client#publish.
      # Raises the same as Client#publish.
      # Returns <tt>Queue</tt> - self
      def publish(body, **properties)
        @client.publish(body, exchange: "", routing_key: @name, **properties)
        self
      end

      # Publish to the queue, without waiting for confirm
      # Parameters are the same as Queue#publish.
      # Options are the same as Queue#publish.
      # Raises the same as Queue#publish.
      # Returns <tt>Queue</tt> - self
      def publish_and_forget(body, **properties)
        @client.publish_and_forget(body, exchange: "", routing_key: @name, **properties)
        self
      end

      # Subscribe/consume from the queue
      # * <tt>no_ack</tt> (<tt>Boolean</tt>) - If true, messages are automatically acknowledged by the server upon delivery.
      #   If false, messages are acknowledged only after the block completes successfully; if the block raises
      #   an exception, the message is rejected and can be optionally requeued.
      #   You can of course handle the ack/reject in the block yourself. (Default: false)
      # * <tt>exclusive</tt> (<tt>Boolean</tt>) - When true only a single consumer can consume from the queue at a time
      # * <tt>prefetch</tt> (<tt>Integer</tt>) - Specify how many messages to prefetch for consumers with no_ack is false
      # * <tt>worker_threads</tt> (<tt>Integer</tt>) - Number of threads processing messages,
      #   0 means that the thread calling this method will be blocked
      # * <tt>requeue_on_reject</tt> (<tt>Boolean</tt>) - If true, messages that are rejected due to an exception in the
      #   block
      #   will be requeued. Only relevant if no_ack is false. (Default: true)
      # * <tt>on_cancel</tt> (<tt>Proc</tt>) - Optional proc that will be called if the consumer is cancelled by the broker
      #   The proc will be called with the consumer tag as the only argument
      # * <tt>arguments</tt> (<tt>Hash</tt>) - Custom arguments to the consumer
      # Yields <tt>Message</tt> - Delivered message from the queue
      # Returns <tt>Consumer</tt> - The consumer object, which can be used to cancel the consumer
      def subscribe(no_ack: false, exclusive: false, prefetch: 1, worker_threads: 1, requeue_on_reject: true,
                    on_cancel: nil, arguments: {})
        @client.subscribe(@name, no_ack:, exclusive:, prefetch:, worker_threads:, on_cancel:, arguments:) do |message|
          yield message
          message.ack unless no_ack
        rescue StandardError => e
          message.reject(requeue: requeue_on_reject) unless no_ack
          raise e
        end
      end

      # Get a message from the queue
      # * <tt>no_ack</tt> (<tt>Boolean</tt>) - When false the message has to be manually acknowledged (or rejected)
      #   (default: false)
      # Returns <tt>Message, nil</tt> - The message from the queue or nil if the queue is empty
      def get(no_ack: false)
        @client.get(@name, no_ack:)
      end

      # Bind the queue to an exchange
      # * <tt>exchange</tt> (<tt>String, Exchange</tt>) - Name of the exchange to bind to, or the exchange object itself
      # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key on which messages that match might be routed (depending on
      #   exchange type)
      # * <tt>arguments</tt> (<tt>Hash</tt>) - Message headers to match on (only relevant for header exchanges)
      # Returns <tt>self</tt>.
      def bind(exchange, binding_key: "", arguments: {})
        exchange = exchange.name unless exchange.is_a?(String)
        @client.bind(queue: @name, exchange:, binding_key:, arguments:)
        self
      end

      # Unbind the queue from an exchange
      # * <tt>exchange</tt> (<tt>String, Exchange</tt>) - Name of the exchange to unbind from, or the exchange object itself
      # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key which the queue is bound to the exchange with
      # * <tt>arguments</tt> (<tt>Hash</tt>) - Arguments matching the binding that's being removed
      # Returns <tt>self</tt>.
      def unbind(exchange, binding_key: "", arguments: {})
        exchange = exchange.name unless exchange.is_a?(String)
        @client.unbind(queue: @name, exchange:, binding_key:, arguments:)
        self
      end

      # Purge/empty the queue
      # Returns <tt>self</tt>.
      def purge
        @client.purge(@name)
        self
      end

      # Delete the queue
      # Returns <tt>nil</tt>.
      def delete
        @client.delete_queue(@name)
        nil
      end
    end
  end
end
