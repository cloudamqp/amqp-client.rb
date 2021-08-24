# frozen_string_literal: true

require "set"
require_relative "client/version"
require_relative "client/connection"

module AMQP
  # AMQP 0-9-1 Client
  class Client
    def initialize(uri, **options)
      @uri = uri
      @options = options

      @queues = {}
      @exchanges = {}
      @subscriptions = Set.new
      @connq = SizedQueue.new(1)
    end

    # Opens an AMQP connection, does not try to reconnect
    def connect(read_loop_thread: true)
      Connection.connect(@uri, **@options.merge(read_loop_thread: read_loop_thread))
    end

    # Opens an AMQP connection using the high level API, will try to reconnect
    def start
      @stopped = false
      Thread.new(connect(read_loop_thread: false)) do |conn|
        Thread.abort_on_exception = true # Raising an unhandled exception is a bug
        loop do
          break if @stopped

          conn ||= connect(read_loop_thread: false)
          Thread.new do
            # restore connection in another thread, read_loop have to run
            conn.channel(1) # reserve channel 1 for publishes
            @subscriptions.each do |queue_name, no_ack, prefetch, wt, args, blk|
              ch = conn.channel
              ch.basic_qos(prefetch)
              ch.basic_consume(queue_name, no_ack: no_ack, worker_threads: wt, arguments: args, &blk)
            end
            @connq << conn
          end
          conn.read_loop # blocks until connection is closed, then reconnect
        rescue AMQP::Client::Error => e
          warn "AMQP-Client reconnect error: #{e.inspect}"
          sleep @options[:reconnect_interval] || 1
        ensure
          conn = nil
        end
      end
      self
    end

    def stop
      @stopped = true
      conn = @connq.pop
      conn.close
      nil
    end

    def queue(name, durable: true, exclusive: false, auto_delete: false, arguments: {})
      raise ArgumentError, "Currently only supports named, durable queues" if name.empty?

      @queues.fetch(name) do
        with_connection do |conn|
          conn.with_channel do |ch| # use a temp channel in case the declaration fails
            ch.queue_declare(name, durable: durable, exclusive: exclusive, auto_delete: auto_delete, arguments: arguments)
          end
        end
        @queues[name] = Queue.new(self, name)
      end
    end

    def exchange(name, type, durable: true, auto_delete: false, internal: false, arguments: {})
      @exchanges.fetch(name) do
        with_connection do |conn|
          conn.with_channel do |ch|
            ch.exchange_declare(name, type, durable: durable, auto_delete: auto_delete, internal: internal, arguments: arguments)
          end
        end
        @exchanges[name] = Exchange.new(self, name)
      end
    end

    # High level representation of an exchange
    class Exchange
      def initialize(client, name)
        @client = client
        @name = name
      end

      def publish(body, routing_key, arguments: {})
        @client.publish(body, @name, routing_key, arguments: arguments)
      end

      # Bind to another exchange
      def bind(exchange, routing_key, arguments: {})
        @client.exchange_bind(@name, exchange, routing_key, arguments: arguments)
      end

      # Unbind from another exchange
      def unbind(exchange, routing_key, arguments: {})
        @client.exchange_unbind(@name, exchange, routing_key, arguments: arguments)
      end

      def delete
        @client.delete_exchange(@name)
      end
    end

    def subscribe(queue_name, no_ack: false, prefetch: 1, worker_threads: 1, arguments: {}, &blk)
      @subscriptions.add? [queue_name, no_ack, prefetch, worker_threads, arguments, blk]

      with_connection do |conn|
        ch = conn.channel
        ch.basic_qos(prefetch)
        ch.basic_consume(queue_name, no_ack: no_ack, worker_threads: worker_threads, arguments: arguments) do |msg|
          blk.call(msg)
        end
      end
    end

    # Publish a (persistent) message and wait for confirmation
    def publish(body, exchange, routing_key, **properties)
      with_connection do |conn|
        properties = { delivery_mode: 2 }.merge!(properties)
        conn.channel(1).basic_publish_confirm(body, exchange, routing_key, **properties)
      end
    end

    # Publish a (persistent) message but don't wait for a confirmation
    def publish_and_forget(body, exchange, routing_key, **properties)
      with_connection do |conn|
        properties = { delivery_mode: 2 }.merge!(properties)
        conn.channel(1).basic_publish(body, exchange, routing_key, **properties)
      end
    end

    def wait_for_confirms
      with_connection do |conn|
        conn.channel(1).wait_for_confirms
      end
    end

    def bind(queue, exchange, routing_key, arguments: {})
      with_connection do |conn|
        conn.channel(1).queue_bind(queue, exchange, routing_key, arguments: arguments)
      end
    end

    def unbind(queue, exchange, routing_key, arguments: {})
      with_connection do |conn|
        conn.channel(1).queue_unbind(queue, exchange, routing_key, arguments: arguments)
      end
    end

    def exchange_bind(destination, source, routing_key, arguments: {})
      with_connection do |conn|
        conn.channel(1).exchange_bind(destination, source, routing_key, arguments: arguments)
      end
    end

    def exchange_unbind(destination, source, routing_key, arguments: {})
      with_connection do |conn|
        conn.channel(1).exchange_unbind(destination, source, routing_key, arguments: arguments)
      end
    end

    def purge(queue)
      with_connection do |conn|
        conn.channel(1).queue_purge(queue)
      end
    end

    def delete_queue(name)
      with_connection do |conn|
        conn.channel(1).queue_delete(name)
        @queues.delete(name)
      end
    end

    def delete_exchange(name)
      with_connection do |conn|
        conn.channel(1).exchange_delete(name)
        @exchanges.delete(name)
      end
    end

    # Queue abstraction
    class Queue
      def initialize(client, name)
        @client = client
        @name = name
      end

      def publish(body, **properties)
        @client.publish(body, "", @name, **properties)
        self
      end

      def subscribe(no_ack: false, prefetch: 1, worker_threads: 1, arguments: {}, &blk)
        @client.subscribe(@name, no_ack: no_ack, prefetch: prefetch, worker_threads: worker_threads, arguments: arguments, &blk)
        self
      end

      def bind(exchange, routing_key, **headers)
        @client.bind(@name, exchange, routing_key, **headers)
        self
      end

      def unbind(exchange, routing_key, **headers)
        @client.unbind(@name, exchange, routing_key, **headers)
        self
      end

      def purge
        @client.purge(@name)
        self
      end

      def delete
        @client.delete_queue(@name)
        nil
      end
    end

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
