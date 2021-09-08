# frozen_string_literal: true

module AMQP
  class Client
    # A message delivered from the broker
    class Message
      # @api private
      def initialize(channel, consumer_tag, delivery_tag, exchange, routing_key, redelivered)
        @channel = channel
        @consumer_tag = consumer_tag
        @delivery_tag = delivery_tag
        @exchange = exchange
        @routing_key = routing_key
        @redelivered = redelivered
        @properties = nil
        @body = ""
      end

      # The channel the message was deliviered to
      # @return [Connection::Channel]
      attr_reader :channel

      # The tag of the consumer the message was deliviered to
      # @return [String]
      # @return [nil] If the message was polled and not deliviered to a consumer
      attr_reader :consumer_tag

      # The delivery tag of the message, used for acknowledge or reject the message
      # @return [Integer]
      attr_reader :delivery_tag

      # Name of the exchange the message was published to
      # @return [String]
      attr_reader :exchange

      # The routing key the message was published with
      # @return [String]
      attr_reader :routing_key

      # True if the message have been delivered before
      # @return [Boolean]
      attr_reader :redelivered

      # Message properties
      # @return [Properties]
      attr_accessor :properties

      # The message body
      # @return [String]
      attr_accessor :body

      # Acknowledge the message
      # @return [nil]
      def ack
        @channel.basic_ack(@delivery_tag)
      end

      # Reject the message
      # @param requeue [Boolean] If true the message will be put back into the queue again, ready to be redelivered
      # @return [nil]
      def reject(requeue: false)
        @channel.basic_reject(@delivery_tag, requeue: requeue)
      end

      # @see #exchange
      # @deprecated
      # @!attribute [r] exchange_name
      # @return [String]
      def exchange_name
        @exchange
      end
    end

    # A published message returned by the broker due to some error
    class ReturnMessage
      # @api private
      def initialize(reply_code, reply_text, exchange, routing_key)
        @reply_code = reply_code
        @reply_text = reply_text
        @exchange = exchange
        @routing_key = routing_key
        @properties = nil
        @body = ""
      end

      # Error code
      # @return [Integer]
      attr_reader :reply_code

      # Description on why the message was returned
      # @return [String]
      attr_reader :reply_text

      # Name of the exchange the message was published to
      # @return [String]
      attr_reader :exchange

      # The routing key the message was published with
      # @return [String]
      attr_reader :routing_key

      # Message properties
      # @return [Properties]
      attr_accessor :properties

      # The message body
      # @return [String]
      attr_accessor :body
    end
  end
end
