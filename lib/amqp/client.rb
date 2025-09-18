# frozen_string_literal: true

require_relative "client/version"
require_relative "client/connection"
require_relative "client/exchange"
require_relative "client/queue"

# AMQP 0-9-1 Protocol, this library only implements the Client
# @see Client
module AMQP
  # AMQP 0-9-1 Client
  # @see Connection
  class Client
    # Create a new Client object, this won't establish a connection yet, use {#connect} or {#start} for that
    # @param uri [String] URL on the format amqp://username:password@hostname/vhost,
    #   use amqps:// for encrypted connection
    # @option options [Boolean] connection_name (PROGRAM_NAME) Set a name for the connection to be able to identify
    #   the client from the broker
    # @option options [Boolean] verify_peer (true) Verify broker's TLS certificate, set to false for self-signed certs
    # @option options [Integer] heartbeat (0) Heartbeat timeout, defaults to 0 and relies on TCP keepalive instead
    # @option options [Integer] frame_max (131_072) Maximum frame size,
    #    the smallest of the client's and the broker's values will be used
    # @option options [Integer] channel_max (2048) Maxium number of channels the client will be allowed to have open.
    #   Maxium allowed is 65_536.  The smallest of the client's and the broker's value will be used.
    def initialize(uri = "", **options)
      @uri = uri
      @options = options
      @queues = {}
      @exchanges = {}
      @subscriptions = Set.new
      @connq = SizedQueue.new(1)
    end

    # @!group Connect and disconnect

    # Establishes and returns a new AMQP connection
    # @see Connection#initialize
    # @return [Connection]
    # @example
    #   connection = AMQP::Client.new("amqps://server.rmq.cloudamqp.com", connection_name: "My connection").connect
    def connect(read_loop_thread: true)
      Connection.new(@uri, read_loop_thread:, **@options)
    end

    # Opens an AMQP connection using the high level API, will try to reconnect if successfully connected at first
    # @return [self]
    # @example
    #   amqp = AMQP::Client.new("amqps://server.rmq.cloudamqp.com")
    #   amqp.start
    #   amqp.queue("foobar")
    def start
      @stopped = false
      Thread.new(connect(read_loop_thread: false)) do |conn|
        Thread.current.abort_on_exception = true # Raising an unhandled exception is a bug
        loop do
          break if @stopped

          conn ||= connect(read_loop_thread: false)
          Thread.new do
            # restore connection in another thread, read_loop have to run
            conn.channel(1) # reserve channel 1 for publishes
            @subscriptions.each do |queue_name, no_ack, prefetch, wt, args, blk|
              ch = conn.channel
              ch.basic_qos(prefetch)
              ch.basic_consume(queue_name, no_ack:, worker_threads: wt, arguments: args, &blk)
            end
            @connq << conn
          end
          conn.read_loop # blocks until connection is closed, then reconnect
        rescue Error => e
          warn "AMQP-Client reconnect error: #{e.inspect}"
          sleep @options[:reconnect_interval] || 1
        ensure
          conn = nil
        end
      end
      self
    end

    # Close the currently open connection
    # @return [nil]
    def stop
      return if @stopped

      @stopped = true
      return unless @connq.size.positive?

      conn = @connq.pop
      conn.close
      nil
    end

    # @!endgroup
    # @!group High level objects

    # Declare a queue
    # @param name [String] Name of the queue
    # @param durable [Boolean] If true the queue will survive broker restarts,
    #   messages in the queue will only survive if they are published as persistent
    # @param auto_delete [Boolean] If true the queue will be deleted when the last consumer stops consuming
    #   (it won't be deleted until at least one consumer has consumed from it)
    # @param arguments [Hash] Custom arguments, such as queue-ttl etc.
    # @return [Queue]
    # @example
    #   amqp = AMQP::Client.new.start
    #   q = amqp.queue("foobar")
    #   q.publish("body")
    def queue(name, durable: true, auto_delete: false, arguments: {})
      raise ArgumentError, "Currently only supports named, durable queues" if name.empty?

      @queues.fetch(name) do
        with_connection do |conn|
          conn.channel(1).queue_declare(name, durable:, auto_delete:, arguments:)
        end
        @queues[name] = Queue.new(self, name)
      end
    end

    # Declare an exchange and return a high level Exchange object
    # @param name [String] Name of the exchange
    # @param type [String] Type of the exchange, one of "direct", "fanout", "topic", "headers" or custom exchange type
    # @param durable [Boolean] If true the exchange will survive broker restarts
    # @param auto_delete [Boolean] If true the exchange will be deleted when the last queue is unbound
    # @param internal [Boolean] If true the exchange will not accept directly published messages
    # @param arguments [Hash] Custom arguments such as alternate-exchange etc.
    # @return [Exchange]
    # @example
    #   amqp = AMQP::Client.new.start
    #   x = amqp.exchange("my.hash.exchange", "x-consistent-hash")
    #   x.publish("body", "routing-key")
    def exchange(name, type, durable: true, auto_delete: false, internal: false, arguments: {})
      @exchanges.fetch(name) do
        with_connection do |conn|
          conn.channel(1).exchange_declare(name, type, durable:, auto_delete:, internal:, arguments:)
        end
        @exchanges[name] = Exchange.new(self, name)
      end
    end

    # Declare a direct exchange and return a high level Exchange object
    # @param name [String] Name of the exchange (defaults to "amq.direct")
    # @see {#exchange} for other parameters
    # @return [Exchange]
    def direct_exchange(name = "amq.direct", **)
      return exchange(name, "direct", **) unless name.empty?

      # Return the default exchange
      @exchanges.fetch(name) do
        @exchanges[name] = Exchange.new(self, name)
      end
    end

    # @deprecated
    # @see {#direct_exchange}
    alias direct direct_exchange

    # Return a high level Exchange object for the default direct exchange
    # @see {#direct} for parameters
    # @return [Exchange]
    def default_exchange(**)
      direct("", **)
    end

    # @deprecated
    # @see default_exchange
    alias default default_exchange

    # Declare a fanout exchange and return a high level Exchange object
    # @param name [String] Name of the exchange (defaults to "amq.fanout")
    # @see {#exchange} for other parameters
    # @return [Exchange]
    def fanout_exchange(name = "amq.fanout", **)
      exchange(name, "fanout", **)
    end

    # @deprecated
    # @see {#fanout_exchange}
    alias fanout fanout_exchange

    # Declare a topic exchange and return a high level Exchange object
    # @param name [String] Name of the exchange (defaults to "amq.topic")
    # @see {#exchange} for other parameters
    # @return [Exchange]
    def topic_exchange(name = "amq.topic", **)
      exchange(name, "topic", **)
    end

    # @deprecated
    # @see {#topic_exchange}
    alias topic topic_exchange

    # Declare a headers exchange and return a high level Exchange object
    # @param name [String] Name of the exchange (defaults to "amq.headers")
    # @see {#exchange} for other parameters
    # @return [Exchange]
    def headers_exchange(name = "amq.headers", **)
      exchange(name, "headers", **)
    end

    # @deprecated
    # @see {#headers_exchange}
    alias headers headers_exchange

    # @!endgroup
    # @!group Publish

    # Publish a (persistent) message and wait for confirmation
    # @param (see Connection::Channel#basic_publish_confirm)
    # @option (see Connection::Channel#basic_publish_confirm)
    # @return (see Connection::Channel#basic_publish_confirm)
    # @raise (see Connection::Channel#basic_publish_confirm)
    def publish(body, exchange, routing_key, **properties)
      with_connection do |conn|
        properties = { delivery_mode: 2 }.merge!(properties)
        conn.channel(1).basic_publish_confirm(body, exchange, routing_key, **properties)
      end
    end

    # Publish a (persistent) message but don't wait for a confirmation
    # @param (see Connection::Channel#basic_publish)
    # @option (see Connection::Channel#basic_publish)
    # @return (see Connection::Channel#basic_publish)
    # @raise (see Connection::Channel#basic_publish)
    def publish_and_forget(body, exchange, routing_key, **properties)
      with_connection do |conn|
        properties = { delivery_mode: 2 }.merge!(properties)
        conn.channel(1).basic_publish(body, exchange, routing_key, **properties)
      end
    end

    # Wait for unconfirmed publishes
    # @return [Boolean] True if successful, false if any message negatively acknowledged
    def wait_for_confirms
      with_connection do |conn|
        conn.channel(1).wait_for_confirms
      end
    end

    # @!endgroup
    # @!group Queue actions

    # Consume messages from a queue
    # @param queue [String] Name of the queue to subscribe to
    # @param no_ack [Boolean] When false messages have to be manually acknowledged (or rejected) (default: false)
    # @param prefetch [Integer] Specify how many messages to prefetch for consumers with no_ack is false (default: 1)
    # @param worker_threads [Integer] Number of threads processing messages (default: 1)
    # @param arguments [Hash] Custom arguments to the consumer
    # @yield [Message] Delivered message from the queue
    # @return [Array<(String, Array<Thread>)>] Returns consumer_tag and an array of worker threads
    # @return [nil]
    def subscribe(queue, no_ack: false, prefetch: 1, worker_threads: 1, arguments: {}, &blk)
      raise ArgumentError, "worker_threads have to be > 0" if worker_threads <= 0

      @subscriptions.add? [queue, no_ack, prefetch, worker_threads, arguments, blk]

      with_connection do |conn|
        ch = conn.channel
        ch.basic_qos(prefetch)
        ch.basic_consume(queue, no_ack:, worker_threads:, arguments:, &blk)
      end
    end

    # Bind a queue to an exchange
    # @param queue [String] Name of the queue to bind
    # @param exchange [String] Name of the exchange to bind to
    # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
    # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
    # @return [nil]
    def bind(queue, exchange, binding_key, arguments: {})
      with_connection do |conn|
        conn.channel(1).queue_bind(queue, exchange, binding_key, arguments:)
      end
    end

    # Unbind a queue from an exchange
    # @param queue [String] Name of the queue to unbind
    # @param exchange [String] Name of the exchange to unbind from
    # @param binding_key [String] Binding key which the queue is bound to the exchange with
    # @param arguments [Hash] Arguments matching the binding that's being removed
    # @return [nil]
    def unbind(queue, exchange, binding_key, arguments: {})
      with_connection do |conn|
        conn.channel(1).queue_unbind(queue, exchange, binding_key, arguments:)
      end
    end

    # Purge a queue
    # @param queue [String] Name of the queue
    # @return [nil]
    def purge(queue)
      with_connection do |conn|
        conn.channel(1).queue_purge(queue)
      end
    end

    # Delete a queue
    # @param name [String] Name of the queue
    # @param if_unused [Boolean] Only delete if the queue doesn't have consumers, raises a ChannelClosed error otherwise
    # @param if_empty [Boolean] Only delete if the queue is empty, raises a ChannelClosed error otherwise
    # @return [Integer] Number of messages in the queue when deleted
    def delete_queue(name, if_unused: false, if_empty: false)
      with_connection do |conn|
        msgs = conn.channel(1).queue_delete(name, if_unused:, if_empty:)
        @queues.delete(name)
        msgs
      end
    end

    # @!endgroup
    # @!group Exchange actions

    # Bind an exchange to an exchange
    # @param destination [String] Name of the exchange to bind
    # @param source [String] Name of the exchange to bind to
    # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
    # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
    # @return [nil]
    def exchange_bind(destination, source, binding_key, arguments: {})
      with_connection do |conn|
        conn.channel(1).exchange_bind(destination, source, binding_key, arguments:)
      end
    end

    # Unbind an exchange from an exchange
    # @param destination [String] Name of the exchange to unbind
    # @param source [String] Name of the exchange to unbind from
    # @param binding_key [String] Binding key which the exchange is bound to the exchange with
    # @param arguments [Hash] Arguments matching the binding that's being removed
    # @return [nil]
    def exchange_unbind(destination, source, binding_key, arguments: {})
      with_connection do |conn|
        conn.channel(1).exchange_unbind(destination, source, binding_key, arguments:)
      end
    end

    # Delete an exchange
    # @param name [String] Name of the exchange
    # @return [nil]
    def delete_exchange(name)
      with_connection do |conn|
        conn.channel(1).exchange_delete(name)
        @exchanges.delete(name)
        nil
      end
    end

    # @!endgroup

    private

    def with_connection
      conn = nil
      loop do
        conn = @connq.pop
        next if conn.closed?

        break
      end
      begin
        yield conn
      ensure
        @connq << conn unless conn.closed?
      end
    end
  end
end
