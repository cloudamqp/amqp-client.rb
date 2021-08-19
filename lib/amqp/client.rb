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
    end

    def stop
      @stopped = true
      conn = @connq.pop
      conn.close
    end

    def queue(name)
      q = @queues[name]
      unless q
        q = @queues[name] = Queue.new(self, name)
        with_connection do |conn|
          conn.channel(1).queue_declare(name)
        end
      end
      q
    end

    def subscribe(queue_name, prefetch: 1, arguments: {}, &blk)
      @subscriptions.add? [queue_name, prefetch, arguments, blk]

      with_connection do |conn|
        ch = conn.channel
        ch.basic_qos(prefetch)
        ch.basic_consume(queue_name, no_ack: false, arguments: arguments) do |msg|
          blk.call(msg)
          ch.basic_ack msg.delivery_tag
        rescue => e
          warn "AMQP-Client subscribe #{queue_name} error: #{e.inspect}"
          sleep 1
          ch.basic_reject msg.delivery_tag, requeue: true
        end
      end
    end

    def publish(body, exchange, routing_key, **properties)
      with_connection do |conn|
        # Use channel 1 for publishes
        conn.channel(1).basic_publish_confirm(body, exchange, routing_key, **properties)
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

      def subscribe(prefetch: 1, arguments: {}, &blk)
        @client.subscribe(@name, prefetch: prefetch, arguments: arguments, &blk)
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
