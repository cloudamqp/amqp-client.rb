# frozen_string_literal: true

module AMQP
  class Client
    # High level representation of an exchange
    class Exchange
      attr_reader :name

      # Should only be initialized from the Client
      # Internal API.
      def initialize(client, name)
        @client = client
        @name = name
      end

      # Publish to the exchange, wait for confirm
      # * <tt>body</tt> (<tt>Object</tt>) - The message body
      #   will be encoded if any matching codec is found in the client's codec registry
      # * <tt>routing_key</tt> (<tt>String</tt>) - Routing key for the message
      # Options are the same as Client#publish.
      # Raises the same as Client#publish.
      # Returns <tt>Exchange</tt> - self
      def publish(body, routing_key: "", **properties)
        @client.publish(body, exchange: @name, routing_key:, **properties)
        self
      end

      # Publish to the exchange, without waiting for confirm
      # Parameters are the same as Exchange#publish.
      # Options are the same as Exchange#publish.
      # Raises the same as Exchange#publish.
      # Returns <tt>Exchange</tt> - self
      def publish_and_forget(body, routing_key: "", **properties)
        @client.publish_and_forget(body, exchange: @name, routing_key:, **properties)
        self
      end

      # Bind to another exchange
      # * <tt>source</tt> (<tt>String, Exchange</tt>) - Name of the exchange to bind to, or the exchange object itself
      # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key on which messages that match might be routed (defaults to
      #   empty string)
      # * <tt>arguments</tt> (<tt>Hash</tt>) - Message headers to match on (only relevant for header exchanges)
      # Returns <tt>Exchange</tt> - self
      def bind(source, binding_key: "", arguments: {})
        source = source.name unless source.is_a?(String)
        @client.exchange_bind(source:, destination: @name, binding_key:, arguments:)
        self
      end

      # Unbind from another exchange
      # * <tt>source</tt> (<tt>String, Exchange</tt>) - Name of the exchange to unbind from, or the exchange object itself
      # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key which the queue is bound to the exchange with (defaults to
      #   empty string)
      # * <tt>arguments</tt> (<tt>Hash</tt>) - Arguments matching the binding that's being removed
      # Returns <tt>Exchange</tt> - self
      def unbind(source, binding_key: "", arguments: {})
        source = source.name unless source.is_a?(String)
        @client.exchange_unbind(source:, destination: @name, binding_key:, arguments:)
        self
      end

      # Delete the exchange
      # Returns <tt>nil</tt>.
      def delete
        @client.delete_exchange(@name)
        nil
      end
    end
  end
end
