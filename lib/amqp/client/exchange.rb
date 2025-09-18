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

      # Publish to the exchange, wait for confirm
      # @param body [Object] The message body, will be encoded according to properties.content_type
      #   and properties.content_encoding if specified (see Client#publish).
      # @param routing_key [String] Routing key for the message
      # @option (see Client#publish)
      # @raise [Error::UnsupportedContentType] If content type is unsupported
      # @raise [Error::UnsupportedContentEncoding] If content encoding is unsupported
      # @raise (see Client#publish)
      # @return [Exchange] self
      def publish(body, routing_key = "", **properties)
        encoded_body = @client.message_coding_strategy.encode_body(body, properties)

        @client.publish(encoded_body, @name, routing_key, **properties)
        self
      end

      # Bind to another exchange
      # @param source [String, Exchange] Name of the exchange to bind to, or the exchange object itself
      # @param binding_key [String] Binding key on which messages that match might be routed (defaults to empty string)
      # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
      # @return [Exchange] self
      def bind(source, binding_key = "", arguments: {})
        source = source.name unless source.is_a?(String)
        @client.exchange_bind(@name, source, binding_key, arguments:)
        self
      end

      # Unbind from another exchange
      # @param source [String, Exchange] Name of the exchange to unbind from, or the exchange object itself
      # @param binding_key [String] Binding key which the queue is bound to the exchange with (defaults to empty string)
      # @param arguments [Hash] Arguments matching the binding that's being removed
      # @return [Exchange] self
      def unbind(source, binding_key = "", arguments: {})
        source = source.name unless source.is_a?(String)
        @client.exchange_unbind(@name, source, binding_key, arguments:)
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
