# frozen_string_literal: true

require_relative "client/version"
require_relative "client/connection"
require_relative "client/exchange"
require_relative "client/queue"
require_relative "client/consumer"
require_relative "client/rpc_client"
require_relative "client/message_codecs"
require_relative "client/message_codec_registry"

# AMQP 0-9-1 Protocol, this library only implements the Client
# @see Client
module AMQP
  # AMQP 0-9-1 Client
  # @see Connection
  class Client
    # Class-level defaults (not automatically inherited by subclasses)
    @codec_registry = MessageCodecRegistry.new
    @strict_coding = false

    # Create a new Client object, this won't establish a connection yet, use {#connect} or {#start} for that
    # @param uri [String] URL on the format amqp://username:password@hostname/vhost,
    #   use amqps:// for encrypted connection
    # @option options [Boolean] connection_name (PROGRAM_NAME) Set a name for the connection to be able to identify
    #   the client from the broker
    # @option options [Boolean] verify_peer (true) Verify broker's TLS certificate, set to false for self-signed certs
    # @option options [Integer] heartbeat (0) Heartbeat timeout, defaults to 0 and relies on TCP keepalive instead
    # @option options [Integer] frame_max (131_072) Maximum frame size,
    #    the smallest of the client's and the broker's values will be used
    # @option options [Integer] channel_max (2048) Maximum number of channels the client will be allowed to have open.
    #   Maximum allowed is 65_536.  The smallest of the client's and the broker's value will be used.
    def initialize(uri = "", **options)
      @uri = uri
      @options = options
      @queues = {}
      @exchanges = {}
      @consumers = {}
      @next_consumer_id = 0
      @connq = SizedQueue.new(1)
      @codec_registry = self.class.codec_registry.dup
      @strict_coding = self.class.strict_coding
      @start_lock = Mutex.new
      @supervisor_started = false
      @stopped = false
    end

    # @!group Connect and disconnect

    # Establishes and returns a new AMQP connection
    # @see Connection#initialize
    # @return [Connection]
    # @example
    #   connection = AMQP::Client.new("amqps://server.rmq.cloudamqp.com", connection_name: "My connection").connect
    def connect(read_loop_thread: true)
      Connection.new(@uri, read_loop_thread:, codec_registry: @codec_registry, strict_coding: @strict_coding, **@options)
    end

    # Opens an AMQP connection using the high level API, will try to reconnect if successfully connected at first
    # @return [self]
    # @example
    #   amqp = AMQP::Client.new("amqps://server.rmq.cloudamqp.com")
    #   amqp.start
    #   amqp.queue("foobar")
    def start
      return self if started?

      @start_lock.synchronize do # rubocop:disable Metrics/BlockLength
        return self if started?

        @supervisor_started = true
        @stopped = false
        Thread.new(connect(read_loop_thread: false)) do |conn|
          Thread.current.abort_on_exception = true # Raising an unhandled exception is a bug
          loop do
            break if @stopped

            conn ||= connect(read_loop_thread: false)

            Thread.new do
              # restore connection in another thread, read_loop have to run
              conn.channel(1) # reserve channel 1 for publishes
              @consumers.each_value do |consumer|
                ch = conn.channel
                ch.basic_qos(consumer.prefetch)
                consume_ok = ch.basic_consume(consumer.queue,
                                              **consumer.basic_consume_args,
                                              &consumer.block)
                # Update the consumer with new channel and consume_ok metadata
                consumer.update_consume_ok(consume_ok)
              end
              @connq << conn
              # Remove consumers whose internal queues were already closed (e.g. cancelled during reconnect window)
              @consumers.delete_if { |_, c| c.closed? }
            end
            conn.read_loop # blocks until connection is closed, then reconnect
          rescue Error => e
            warn "AMQP-Client reconnect error: #{e.inspect}"
            sleep @options[:reconnect_interval] || 1
          ensure
            @connq.clear
            conn = nil
          end
        end
      end
      self
    end

    # Close the currently open connection and stop the supervision / reconnection logic.
    # @return [nil]
    def stop
      return if @stopped && !@supervisor_started

      @stopped = true
      return unless @connq.size.positive?

      conn = @connq.pop
      conn.close
      nil
    end

    # Check if the client is connected
    # @return [Boolean] true if connected or currently trying to connect, false otherwise
    def started?
      @supervisor_started && !@stopped
    end

    # @!endgroup
    # @!group High level objects

    # Declare a queue
    # @param name [String] Name of the queue
    # @param durable [Boolean] If true the queue will survive broker restarts,
    #   messages in the queue will only survive if they are published as persistent
    # @param auto_delete [Boolean] If true the queue will be deleted when the last consumer stops consuming
    #   (it won't be deleted until at least one consumer has consumed from it)
    # @param exclusive [Boolean] If true raise an exception if the exchange doesn't already exists
    # @param arguments [Hash] Custom arguments, such as queue-ttl etc.
    # @return [Queue]
    # @example
    #   amqp = AMQP::Client.new.start
    #   q = amqp.queue("foobar")
    #   q.publish("body")
    def queue(name, durable: true, auto_delete: false, exclusive: false, passive: false, arguments: {})
      raise ArgumentError, "Currently only supports named, durable queues" if name.empty?

      @queues.fetch(name) do
        with_connection do |conn|
          conn.channel(1).queue_declare(name, durable:, auto_delete:, exclusive:, passive:, arguments:)
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
    #   x = amqp.exchange("my.hash.exchange", type: "x-consistent-hash")
    #   x.publish("body", routing_key: "routing-key")
    def exchange(name, type:, durable: true, auto_delete: false, internal: false, arguments: {})
      @exchanges.fetch(name) do
        with_connection do |conn|
          conn.channel(1).exchange_declare(name, type:, durable:, auto_delete:, internal:, arguments:)
        end
        @exchanges[name] = Exchange.new(self, name)
      end
    end

    # Declare a direct exchange and return a high level Exchange object
    # @param name [String] Name of the exchange (defaults to "amq.direct")
    # @see #exchange for other parameters
    # @return [Exchange]
    def direct_exchange(name = "amq.direct", **)
      return exchange(name, type: "direct", **) unless name.empty?

      # Return the default exchange
      @exchanges.fetch(name) do
        @exchanges[name] = Exchange.new(self, name)
      end
    end

    # @deprecated
    # @see #direct_exchange
    alias direct direct_exchange

    # Return a high level Exchange object for the default direct exchange
    # @see #direct for parameters
    # @return [Exchange]
    def default_exchange(**)
      direct("", **)
    end

    # @deprecated
    # @see #default_exchange
    alias default default_exchange

    # Declare a fanout exchange and return a high level Exchange object
    # @param name [String] Name of the exchange (defaults to "amq.fanout")
    # @see #exchange for other parameters
    # @return [Exchange]
    def fanout_exchange(name = "amq.fanout", **)
      exchange(name, type: "fanout", **)
    end

    # @deprecated
    # @see #fanout_exchange
    alias fanout fanout_exchange

    # Declare a topic exchange and return a high level Exchange object
    # @param name [String] Name of the exchange (defaults to "amq.topic")
    # @see #exchange for other parameters
    # @return [Exchange]
    def topic_exchange(name = "amq.topic", **)
      exchange(name, type: "topic", **)
    end

    # @deprecated
    # @see #topic_exchange
    alias topic topic_exchange

    # Declare a headers exchange and return a high level Exchange object
    # @param name [String] Name of the exchange (defaults to "amq.headers")
    # @see #exchange for other parameters
    # @return [Exchange]
    def headers_exchange(name = "amq.headers", **)
      exchange(name, type: "headers", **)
    end

    # @deprecated
    # @see #headers_exchange
    alias headers headers_exchange

    # @!endgroup
    # @!group Publish

    # Publish a (persistent) message and wait for confirmation
    # @param body [Object] The message body
    #   will be encoded if any matching codec is found in the client's codec registry
    # @param exchange [String] Name of the exchange to publish to
    # @param routing_key [String] Routing key for the message
    # @option (see Connection::Channel#basic_publish_confirm)
    # @return [nil]
    # @raise (see Connection::Channel#basic_publish_confirm)
    # @raise [Error::PublishNotConfirmed] If the message was not confirmed by the broker
    # @raise [Error::UnsupportedContentType] If content type is unsupported
    # @raise [Error::UnsupportedContentEncoding] If content encoding is unsupported
    def publish(body, exchange:, routing_key: "", **properties)
      with_connection do |conn|
        properties = { delivery_mode: 2 }.merge!(properties)
        body = serialize_and_encode_body(body, properties)
        result = conn.channel(1).basic_publish_confirm(body, exchange:, routing_key:, **properties)
        raise Error::PublishNotConfirmed unless result

        nil
      end
    end

    # Publish a (persistent) message but don't wait for a confirmation
    # @param (see Connection::Channel#basic_publish)
    # @option (see Connection::Channel#basic_publish)
    # @return (see Connection::Channel#basic_publish)
    # @raise (see Connection::Channel#basic_publish)
    # @raise [Error::UnsupportedContentType] If content type is unsupported
    # @raise [Error::UnsupportedContentEncoding] If content encoding is unsupported
    def publish_and_forget(body, exchange:, routing_key: "", **properties)
      with_connection do |conn|
        properties = { delivery_mode: 2 }.merge!(properties)
        body = serialize_and_encode_body(body, properties)
        conn.channel(1).basic_publish(body, exchange:, routing_key:, **properties)
      end
    end

    # Wait for unconfirmed publishes
    # @return [Boolean] True if successful, false if any message negatively acknowledged
    def wait_for_confirms
      with_connection do |conn|
        conn.channel(1).wait_for_confirms
      end
    end

    # @!group Queue actions

    # Consume messages from a queue
    # @param queue [String] Name of the queue to subscribe to
    # @param no_ack [Boolean] When false messages have to be manually acknowledged (or rejected) (default: false)
    # @param prefetch [Integer] Specify how many messages to prefetch for consumers with no_ack is false (default: 1)
    # @param worker_threads [Integer] Number of threads processing messages (default: 1)
    # @param on_cancel [Proc] Optional proc that will be called if the consumer is cancelled by the broker
    #   The proc will be called with the consumer tag as the only argument
    # @param arguments [Hash] Custom arguments to the consumer
    # @yield [Message] Delivered message from the queue
    # @return [Consumer] The consumer object, which can be used to cancel the consumer
    def subscribe(queue, exclusive: false, no_ack: false, prefetch: 1, worker_threads: 1,
                  on_cancel: nil, arguments: {}, &blk)
      raise ArgumentError, "worker_threads have to be > 0" if worker_threads <= 0

      with_connection do |conn|
        ch = conn.channel
        ch.basic_qos(prefetch)
        consumer_id = @next_consumer_id += 1
        on_cancel_proc = proc do |tag|
          @consumers.delete(consumer_id)
          on_cancel&.call(tag)
        end
        basic_consume_args = { exclusive:, no_ack:, worker_threads:, on_cancel: on_cancel_proc, arguments: }
        consume_ok = ch.basic_consume(queue, **basic_consume_args, &blk)
        consumer = Consumer.new(client: self, channel_id: ch.id, id: consumer_id, block: blk,
                                queue:, consume_ok:, prefetch:, basic_consume_args:)
        @consumers[consumer_id] = consumer
        consumer
      end
    end

    # Get a message from a queue
    # @param queue [String] Name of the queue to get the message from
    # @param no_ack [Boolean] When false the message has to be manually acknowledged (or rejected) (default: false)
    # @return [Message, nil] The message from the queue or nil if the queue is empty
    def get(queue, no_ack: false)
      with_connection do |conn|
        ch = conn.channel
        ch.basic_get(queue, no_ack:)
      end
    end

    # Bind a queue to an exchange
    # @param queue [String] Name of the queue to bind
    # @param exchange [String] Name of the exchange to bind to
    # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
    # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
    # @return [nil]
    def bind(queue:, exchange:, binding_key: "", arguments: {})
      with_connection do |conn|
        conn.channel(1).queue_bind(queue, exchange:, binding_key:, arguments:)
      end
    end

    # Unbind a queue from an exchange
    # @param queue [String] Name of the queue to unbind
    # @param exchange [String] Name of the exchange to unbind from
    # @param binding_key [String] Binding key which the queue is bound to the exchange with
    # @param arguments [Hash] Arguments matching the binding that's being removed
    # @return [nil]
    def unbind(queue:, exchange:, binding_key: "", arguments: {})
      with_connection do |conn|
        conn.channel(1).queue_unbind(queue, exchange:, binding_key:, arguments:)
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
    # @param source [String] Name of the exchange to bind to
    # @param destination [String] Name of the exchange to bind
    # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
    # @param arguments [Hash] Message headers to match on (only relevant for header exchanges)
    # @return [nil]
    def exchange_bind(source:, destination:, binding_key: "", arguments: {})
      with_connection do |conn|
        conn.channel(1).exchange_bind(destination:, source:, binding_key:, arguments:)
      end
    end

    # Unbind an exchange from an exchange
    # @param source [String] Name of the exchange to unbind from
    # @param destination [String] Name of the exchange to unbind
    # @param binding_key [String] Binding key which the exchange is bound to the exchange with
    # @param arguments [Hash] Arguments matching the binding that's being removed
    # @return [nil]
    def exchange_unbind(source:, destination:, binding_key: "", arguments: {})
      with_connection do |conn|
        conn.channel(1).exchange_unbind(destination:, source:, binding_key:, arguments:)
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
    # @!group RPC

    # Create a RPC server for a single method/function/procedure
    # @param method [String, Symbol] name of the RPC method to host (i.e. queue name on the server side)
    # @param worker_threads [Integer] number of threads that process requests
    # @param durable [Boolean] If true the queue will survive broker restarts
    # @param auto_delete [Boolean] If true the queue will be deleted when the last consumer stops consuming
    #   (it won't be deleted until at least one consumer has consumed from it)
    # @param arguments [Hash] Custom arguments, such as queue-ttl etc.
    # @yield Block that processes the RPC request messages
    # @yieldparam [String] The body of the request message
    # @yieldreturn [String] The response message body
    # @return (see #subscribe)
    def rpc_server(method, worker_threads: 1, durable: true, auto_delete: false, arguments: {}, &_)
      queue(method.to_s, durable:, auto_delete:, arguments:)
        .subscribe(prefetch: worker_threads, worker_threads:) do |msg|
          result = yield msg.parse
          properties = { content_type: msg.properties.content_type,
                         content_encoding: msg.properties.content_encoding }
          result_body = serialize_and_encode_body(result, properties)

          msg.channel.basic_publish(result_body, exchange: "", routing_key: msg.properties.reply_to,
                                                 correlation_id: msg.properties.correlation_id, **properties)
          msg.ack
        rescue StandardError
          msg.reject(requeue: false)
          raise
        end
    end

    # Do a RPC call, sends a messages, waits for a response
    # @param method [String, Symbol] name of the RPC method to call (i.e. queue name on the server side)
    # @param arguments [String] arguments/body to the call
    # @param timeout [Numeric, nil] Number of seconds to wait for a response
    # @option (see Client#publish)
    # @return [String] Returns the result from the call
    # @raise [Timeout::Error] if no response is received within the timeout period
    def rpc_call(method, arguments, timeout: nil, **properties)
      ch = with_connection(&:channel)
      begin
        msg = ch.basic_consume_once("amq.rabbitmq.reply-to", timeout:) do
          body = serialize_and_encode_body(arguments, properties)
          ch.basic_publish(body, exchange: "", routing_key: method.to_s,
                                 reply_to: "amq.rabbitmq.reply-to", **properties)
        end
        msg.parse
      ensure
        ch.close
      end
    end

    # Create a reusable RPC client
    # @return [RPCClient]
    def rpc_client
      ch = with_connection(&:channel)
      RPCClient.new(ch).start
    end

    # @!endgroup
    # @!group Message coding

    class << self
      # Get the default codec registry
      # @return [MessageCodecRegistry]
      attr_reader :codec_registry

      # Get/set if coding should default to strict, i.e. if the client should raise on unknown codecs
      attr_accessor :strict_coding

      # We need to set the subclass's codec registry and strict coding default
      # because these are class instance variables, hence not inherited.
      # @api private
      def inherited(subclass)
        super
        subclass.instance_variable_set(:@codec_registry, @codec_registry.dup)
        subclass.strict_coding = @strict_coding
      end
    end

    # Get the codec registry for this instance
    # @return [MessageCodecRegistry]
    attr_reader :codec_registry

    # Get/set if condig should be strict, i.e. if the client should raise on unknown codecs
    attr_accessor :strict_coding

    # @!endgroup
    #
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

    # @api private
    def cancel_consumer(consumer)
      @consumers.delete(consumer.id)
      with_connection do |conn|
        conn.channel(consumer.channel_id).basic_cancel(consumer.tag)
      end
    end

    private

    def serialize_and_encode_body(body, properties)
      body = serialize_body(body, properties)
      encode_body(body, properties)
    end

    def encode_body(body, properties)
      ce = properties[:content_encoding]
      coder = @codec_registry.find_coder(ce)

      return coder.encode(body, properties) if coder

      is_unsupported = ce && ce != ""
      raise Error::UnsupportedContentEncoding, ce if is_unsupported && @strict_coding

      body
    end

    def serialize_body(body, properties)
      return body if body.is_a?(String)

      ct = properties[:content_type]
      parser = @codec_registry.find_parser(ct)

      return parser.serialize(body, properties) if parser

      is_unsupported = ct && ct != "" && ct != "text/plain"
      raise Error::UnsupportedContentType, ct if is_unsupported && @strict_coding

      body.to_s
    end
  end
end
