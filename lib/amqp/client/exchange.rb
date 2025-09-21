# frozen_string_literal: true

module AMQP
  class Client
    # High level representation of an exchange
    class Exchange
      attr_reader :name

      # Should only be initialized from the Client
      # @api private
      def initialize(client, name)
        @client = client
        @name = name
      end

      # Publish to the exchange
      # @param body [String] The message body
      # @param routing_key [String] The routing key of the message,
      #   the exchange may use this when routing the message to bound queues (defaults to empty string)
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
      # @return [Exchange] self
      def publish(body, routing_key: "", **properties)
        @client.publish(body, exchange: @name, routing_key:, **properties)
        self
      end

      # Bind to another exchange
      # @param source [String, Exchange] Name of the exchange to bind to, or the exchange object itself
      # @param binding_key [String] Binding key on which messages that match might be routed (defaults to empty string)
      # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
      # @return [Exchange] self
      def bind(source, binding_key: "", arguments: {})
        source = source.name unless source.is_a?(String)
        @client.exchange_bind(source:, destination: @name, binding_key:, arguments:)
        self
      end

      # Unbind from another exchange
      # @param source [String, Exchange] Name of the exchange to unbind from, or the exchange object itself
      # @param binding_key [String] Binding key which the queue is bound to the exchange with (defaults to empty string)
      # @param arguments [Hash] Arguments matching the binding that's being removed
      # @return [Exchange] self
      def unbind(source, binding_key: "", arguments: {})
        source = source.name unless source.is_a?(String)
        @client.exchange_unbind(source:, destination: @name, binding_key:, arguments:)
        self
      end

      # Delete the exchange
      # @return [nil]
      def delete
        @client.delete_exchange(@name)
        nil
      end
    end
  end
end
