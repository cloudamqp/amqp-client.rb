# frozen_string_literal: true

module AMQP
  class Client
    # A message delivered from the broker
    Message = Struct.new(:channel, :delivery_tag, :exchange_name, :routing_key, :properties, :body, :redelivered, :consumer_tag) do
      # Acknowledge the message
      def ack
        channel.basic_ack(delivery_tag)
      end

      # Reject the message
      def reject(requeue: false)
        channel.basic_reject(delivery_tag, requeue: requeue)
      end
    end

    # A published message returned by the broker due to some error
    ReturnMessage = Struct.new(:reply_code, :reply_text, :exchange, :routing_key, :properties, :body)
  end
end
