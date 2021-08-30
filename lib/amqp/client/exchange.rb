# frozen_string_literal: true

module AMQP
  class Client
    # High level representation of an exchange
    class Exchange
      def initialize(client, name)
        @client = client
        @name = name
      end

      # Publish to the exchange
      # @return [Exchange] self
      def publish(body, routing_key, arguments: {})
        @client.publish(body, @name, routing_key, arguments: arguments)
        self
      end

      # Bind to another exchange
      # @return [Exchange] self
      def bind(exchange, routing_key, arguments: {})
        @client.exchange_bind(@name, exchange, routing_key, arguments: arguments)
        self
      end

      # Unbind from another exchange
      # @return [Exchange] self
      def unbind(exchange, routing_key, arguments: {})
        @client.exchange_unbind(@name, exchange, routing_key, arguments: arguments)
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
