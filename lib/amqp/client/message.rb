# frozen_string_literal: true

module AMQP
  class Client
    # A message delivered from the broker
    Message = Struct.new(:channel, :delivery_tag, :exchange_name, :routing_key, :properties, :body, :redelivered, :consumer_tag) do
      # @!attribute channel
      #   @return [Connection::Channel] The channel the message was deliviered to
      # @!attribute delivery_tag
      #   @return [Integer] The delivery tag of the message, used for acknowledge or reject the message
      # @!attribute exchange_name
      #   @return [String] Name of the exchange the message was published to
      # @!attribute routing_key
      #   @return [String] The routing key the message was published with
      # @!attribute properties
      #   @return [Properties]
      # @!attribute body
      #   @return [String] The message body
      # @!attribute redelivered
      #   @return [Boolean] True if the message have been delivered before
      # @!attribute consumer_tag
      #   @return [String] The tag of the consumer the message was deliviered to
      #   @return [nil] Nil if the message was polled and not deliviered to a consumer

      # Acknowledge the message
      # @return [void]
      def ack
        channel.basic_ack(delivery_tag)
      end

      # Reject the message
      # @param requeue [Boolean] If true the message put back into the queue again, ready to be redelivered
      # @return [void]
      def reject(requeue: false)
        channel.basic_reject(delivery_tag, requeue: requeue)
      end
    end

    # A published message returned by the broker due to some error
    ReturnMessage = Struct.new(:reply_code, :reply_text, :exchange, :routing_key, :properties, :body)
  end
end
