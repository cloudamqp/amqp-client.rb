# frozen_string_literal: true

module AMQP
  Message = Struct.new(:exchange_name, :routing_key, :properties, :body, :redelivered)
end
