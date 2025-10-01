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
      # @param method [String, Symbol] name of the method to call (i.e. queue name on the server side)
      # @param arguments [String] arguments/body to the call
      # @param timeout [Numeric, nil] Number of seconds to wait for a response
      # @option (see Client#publish)
      # @raise [Timeout::Error] if no response is received within the timeout period
      # @return [String] Returns the result from the call
      def call(method, arguments, timeout: nil, **properties)
        correlation_id = @lock.synchronize { @correlation_id += 1 }.to_s(36)
        @ch.basic_publish(arguments, exchange: "", routing_key: method.to_s,
                                     reply_to: "amq.rabbitmq.reply-to", correlation_id:, **properties)
        Timeout.timeout(timeout) do # Timeout the whole loop if we never find the right correlation_id
          loop do
            msg = @messages.pop(timeout:) # Timeout individual pop to avoid blocking forever
            raise Timeout::Error if msg.nil? && timeout

            return msg.body if msg.properties.correlation_id == correlation_id

            @messages.push msg
          end
        end
      rescue Timeout::Error
        raise Timeout::Error, "No response received in #{timeout} seconds"
      end

      # Closes the channel used by the RPCClient
      def close
        @ch.close
        @messages.close
      end
    end
  end
end
