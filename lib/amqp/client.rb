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
      @subscriptions = Set.new
      @connq = SizedQueue.new(1)
    end

    def connect(read_loop_thread: true)
      Connection.connect(@uri, **@options.merge(read_loop_thread: read_loop_thread))
    end

    def start
      @stopped = false
      Thread.new do
        loop do
          break if @stopped

          conn = connect(read_loop_thread: false)
          Thread.new do
            # restore connection in another thread, read_loop have to run
            conn.channel(1) # reserve channel 1 for publishes
            @subscriptions.each { |args| subscribe(*args) }
            @connq << conn
          end
          conn.read_loop # blocks until connection is closed, then reconnect
        rescue => e
          warn "AMQP-Client reconnect error: #{e.inspect}"
          sleep @options[:reconnect_interval] || 1
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

    def queue(name, arguments: {})
      raise ArgumentError, "Currently only supports named, durable queues" if name.empty?

      @queues.fetch(name) do
        with_connection do |conn|
          conn.with_channel do |ch| # use a temp channel in case the declaration fails
            ch.queue_declare(name, arguments: arguments)
          end
        end
        @queues[name] = Queue.new(self, name)
      end
    end

    def subscribe(queue_name, no_ack: false, prefetch: 1, worker_threads: 1, arguments: {}, &blk)
      @subscriptions.add? [queue_name, no_ack, prefetch, arguments, blk]

      with_connection do |conn|
        ch = conn.channel
        ch.basic_qos(prefetch)
        ch.basic_consume(queue_name, no_ack: no_ack, worker_threads: worker_threads, arguments: arguments) do |msg|
          blk.call(msg)
        end
      end
    end

    def publish(body, exchange, routing_key, **properties)
      with_connection do |conn|
        # Use channel 1 for publishes
        conn.channel(1).basic_publish_confirm(body, exchange, routing_key, **properties)
      rescue
        conn.channel(1) # reopen channel 1 if it raised
        raise
      end
    rescue => e
      warn "AMQP-Client error publishing, retrying (#{e.inspect})"
      retry
    end

    def bind(queue, exchange, routing_key, **headers)
      with_connection do |conn|
        conn.channel(1).queue_bind(queue, exchange, routing_key, **headers)
      end
    end

    def unbind(queue, exchange, routing_key, **headers)
      with_connection do |conn|
        conn.channel(1).queue_unbind(queue, exchange, routing_key, **headers)
      end
    end

    def purge(queue)
      with_connection do |conn|
        conn.channel(1).queue_purge(queue)
      end
    end

    def delete_queue(queue)
      with_connection do |conn|
        conn.channel(1).queue_delete(queue)
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
