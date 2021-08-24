# frozen_string_literal: true

module AMQP
  Message = Struct.new(:channel, :delivery_tag, :exchange_name, :routing_key, :properties, :body, :redelivered, :consumer_tag) do
    def ack
      channel.basic_ack(delivery_tag)
    end

    def reject(requeue: false)
      channel.basic_reject(delivery_tag, requeue: requeue)
    end
  end

  ReturnMessage = Struct.new(:reply_code, :reply_text, :exchange, :routing_key, :properties, :body)
end
