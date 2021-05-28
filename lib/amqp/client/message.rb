# frozen_string_literal: true

module AMQP
  Message = Struct.new(:delivery_tag, :exchange_name, :routing_key, :properties, :body, :redelivered, :consumer_tag)
end
