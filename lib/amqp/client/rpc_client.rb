# frozen_string_literal: true

module AMQP
  class Client
    # Reusable RPC client, when RPC performance is important
    class RPCClient
      # @param channel [AMQP::Client::Connection::Channel] the channel to use for the RPC calls
      def initialize(channel)
        @ch = channel
        @correlation_id = 0
        @lock = Mutex.new
        @messages = ::Queue.new
      end

      # Start listening for responses from the RPC calls
      # @return [self]
      def start
        @ch.basic_consume("amq.rabbitmq.reply-to") do |msg|
          @messages.push msg
        end
        self
      end

      # Do a RPC call, sends a messages, waits for a response
      # @param arguments [String] arguments/body to the call
      # @param queue [String] name of the queue that RPC call will be sent to
      # @return [String] Returns the result from the call
      def call(arguments, queue:)
        correlation_id = @lock.synchronize { @correlation_id += 1 }.to_s(36)
        @ch.basic_publish(arguments, exchange: "", routing_key: queue,
                                     reply_to: "amq.rabbitmq.reply-to", correlation_id:)
        loop do
          msg = @messages.pop
          return msg.body if msg.properties.correlation_id == correlation_id

          @messages.push msg
        end
      end

      # Closes the channel used by the RPCClient
      def close
        @ch.close
        @messages.close
      end
    end
  end
end
