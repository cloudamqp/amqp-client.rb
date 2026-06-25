# frozen_string_literal: true

require_relative "client/version"
require_relative "client/connection"
require_relative "client/exchange"
require_relative "client/queue"
require_relative "client/consumer"
require_relative "client/rpc_client"
require_relative "client/message_codecs"
require_relative "client/message_codec_registry"
require_relative "client/configuration"

# AMQP 0-9-1 Protocol, this library only implements the Client
# See Client.
module AMQP
  # AMQP 0-9-1 Client
  # See Connection.
  class Client
    # Class-level codec registry
    @codec_registry = MessageCodecRegistry.new
    # Class-level configuration
    @config = Configuration.new(@codec_registry)

    # Create a new Client object, this won't establish a connection yet, use #connect or #start for that
    # * <tt>uri</tt> (<tt>String</tt>) - URL on the format amqp://username:password@hostname/vhost,
    #   use amqps:// for encrypted connection
    # * <tt>connection_name</tt> (<tt>Boolean</tt>, default: <tt>PROGRAM_NAME</tt>) - Set a name for the connection to be
    #   able to identify
    #   the client from the broker
    # * <tt>verify_peer</tt> (<tt>Boolean</tt>, default: <tt>true</tt>) - Verify broker's TLS certificate, set to false for
    #   self-signed certs
    # * <tt>heartbeat</tt> (<tt>Integer</tt>, default: <tt>0</tt>) - Heartbeat timeout, defaults to 0 and relies on TCP
    #   keepalive instead
    # * <tt>frame_max</tt> (<tt>Integer</tt>, default: <tt>131_072</tt>) - Maximum frame size; the smallest of the
    #   client's and the broker's values will be used
    # * <tt>channel_max</tt> (<tt>Integer</tt>, default: <tt>2048</tt>) - Maximum number of channels the client will be
    #   allowed to have open.
    #   Maximum allowed is 65_536. The smallest of the client's and the broker's values will be used.
    # * <tt>logger</tt> (<tt>#info, #warn, #error</tt>, default: <tt>nil</tt>) - Logger for #start lifecycle events
    #   (connected/reconnected/disconnected/reconnect errors). When nil, reconnect errors are
    #   written to stderr via Kernel#warn for backwards compatibility.
    def initialize(uri = "", **options)
      @uri = uri
      @options = options
      @logger = options[:logger]
      @name = parse_name(uri)
      @queues = {}
      @exchanges = {}
      @consumers = {}
      @next_consumer_id = 0
      @connq = SizedQueue.new(1)
      @codec_registry = self.class.codec_registry.dup
      @strict_coding = self.class.config.strict_coding
      @default_content_encoding = self.class.config.default_content_encoding
      @default_content_type = self.class.config.default_content_type
      @start_lock = Mutex.new
      @supervisor_started = false
      @stopped = false
    end

    # :section: Connect and disconnect

    # Establishes and returns a new AMQP connection
    # See Connection#initialize.
    # Returns <tt>Connection</tt>.
    # === Example
    #   connection = AMQP::Client.new("amqps://server.rmq.cloudamqp.com", connection_name: "My connection").connect
    def connect(read_loop_thread: true)
      Connection.new(@uri, read_loop_thread:, name: @name,
                           codec_registry: @codec_registry, strict_coding: @strict_coding, **@options)
    end

    # Opens an AMQP connection using the high level API, will try to reconnect if successfully connected at first
    # Returns <tt>self</tt>.
    # === Example
    #   amqp = AMQP::Client.new("amqps://server.rmq.cloudamqp.com")
    #   amqp.start
    #   amqp.queue("foobar")
    def start
      return self if started?

      @start_lock.synchronize do # rubocop:disable Metrics/BlockLength
        return self if started?

        @supervisor_started = true
        @stopped = false
        initial_conn = connect(read_loop_thread: false)
        log_lifecycle(:info, "connected")
        supervisor = Thread.new(initial_conn) do |conn| # rubocop:disable Metrics/BlockLength
          Thread.current.abort_on_exception = true # Raising an unhandled exception is a bug
          loop do # rubocop:disable Metrics/BlockLength
            break if @stopped

            unless conn
              conn = connect(read_loop_thread: false)
              log_lifecycle(:info, "reconnected")
            end

            setup = Thread.new do
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
            setup.name = thread_name("reconnect_setup")
            conn.read_loop # blocks until connection is closed, then reconnect
            log_lifecycle(:warn, "disconnected")
          rescue Error => e
            log_reconnect_error(e)
            sleep @options[:reconnect_interval] || 1
          ensure
            @connq.clear
            conn = nil
          end
        end
        supervisor.name = thread_name("supervisor")
      end
      self
    end

    # Close the currently open connection and stop the supervision / reconnection logic.
    # Returns <tt>nil</tt>.
    def stop
      return if @stopped && !@supervisor_started

      @stopped = true
      return unless @connq.size.positive?

      conn = @connq.pop
      conn.close
      nil
    end

    # Check if the client is connected
    # Returns <tt>Boolean</tt> - true if connected or currently trying to connect, false otherwise
    def started?
      @supervisor_started && !@stopped
    end

    # :section: High level objects

    # Declare a queue
    # * <tt>name</tt> (<tt>String</tt>) - Name of the queue
    # * <tt>durable</tt> (<tt>Boolean</tt>) - If true the queue will survive broker restarts,
    #   messages in the queue will only survive if they are published as persistent
    # * <tt>auto_delete</tt> (<tt>Boolean</tt>) - If true the queue will be deleted when the last consumer stops consuming
    #   (it won't be deleted until at least one consumer has consumed from it)
    # * <tt>exclusive</tt> (<tt>Boolean</tt>) - If true the queue will be deleted when the connection is closed
    # * <tt>passive</tt> (<tt>Boolean</tt>) - If true an exception will be raised if the queue doesn't already exists
    # * <tt>arguments</tt> (<tt>Hash</tt>) - Custom arguments, such as queue-ttl etc.
    # Returns <tt>Queue</tt>.
    # === Example
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
    # * <tt>name</tt> (<tt>String</tt>) - Name of the exchange
    # * <tt>type</tt> (<tt>String</tt>) - Type of the exchange, one of "direct", "fanout", "topic", "headers" or custom
    #   exchange type
    # * <tt>durable</tt> (<tt>Boolean</tt>) - If true the exchange will survive broker restarts
    # * <tt>auto_delete</tt> (<tt>Boolean</tt>) - If true the exchange will be deleted when the last queue is unbound
    # * <tt>internal</tt> (<tt>Boolean</tt>) - If true the exchange will not accept directly published messages
    # * <tt>arguments</tt> (<tt>Hash</tt>) - Custom arguments such as alternate-exchange etc.
    # Returns <tt>Exchange</tt>.
    # === Example
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
    # * <tt>name</tt> (<tt>String</tt>) - Name of the exchange (defaults to "amq.direct")
    # See #exchange for other parameters.
    # Returns <tt>Exchange</tt>.
    def direct_exchange(name = "amq.direct", **)
      return exchange(name, type: "direct", **) unless name.empty?

      # Return the default exchange
      @exchanges.fetch(name) do
        @exchanges[name] = Exchange.new(self, name)
      end
    end

    # Deprecated.
    # See #direct_exchange.
    alias direct direct_exchange

    # Return a high level Exchange object for the default direct exchange
    # See #direct for parameters.
    # Returns <tt>Exchange</tt>.
    def default_exchange(**)
      direct("", **)
    end

    # Deprecated.
    # See #default_exchange.
    alias default default_exchange

    # Declare a fanout exchange and return a high level Exchange object
    # * <tt>name</tt> (<tt>String</tt>) - Name of the exchange (defaults to "amq.fanout")
    # See #exchange for other parameters.
    # Returns <tt>Exchange</tt>.
    def fanout_exchange(name = "amq.fanout", **)
      exchange(name, type: "fanout", **)
    end

    # Deprecated.
    # See #fanout_exchange.
    alias fanout fanout_exchange

    # Declare a topic exchange and return a high level Exchange object
    # * <tt>name</tt> (<tt>String</tt>) - Name of the exchange (defaults to "amq.topic")
    # See #exchange for other parameters.
    # Returns <tt>Exchange</tt>.
    def topic_exchange(name = "amq.topic", **)
      exchange(name, type: "topic", **)
    end

    # Deprecated.
    # See #topic_exchange.
    alias topic topic_exchange

    # Declare a headers exchange and return a high level Exchange object
    # * <tt>name</tt> (<tt>String</tt>) - Name of the exchange (defaults to "amq.headers")
    # See #exchange for other parameters.
    # Returns <tt>Exchange</tt>.
    def headers_exchange(name = "amq.headers", **)
      exchange(name, type: "headers", **)
    end

    # Deprecated.
    # See #headers_exchange.
    alias headers headers_exchange

    # :section: Publish

    # Publish a (persistent) message and wait for confirmation
    # * <tt>body</tt> (<tt>Object</tt>) - The message body
    #   will be encoded if any matching codec is found in the client's codec registry
    # * <tt>exchange</tt> (<tt>String</tt>) - Name of the exchange to publish to
    # * <tt>routing_key</tt> (<tt>String</tt>) - Routing key for the message
    # Options are the same as Connection::Channel#basic_publish_confirm.
    # Returns <tt>nil</tt>.
    # Raises the same as Connection::Channel#basic_publish_confirm.
    # Raises <tt>Error::PublishNotConfirmed</tt> - If the message was not confirmed by the broker
    # Raises <tt>Error::UnsupportedContentType</tt> - If content type is unsupported
    # Raises <tt>Error::UnsupportedContentEncoding</tt> - If content encoding is unsupported
    def publish(body, exchange:, routing_key: "", **properties)
      with_connection do |conn|
        properties[:delivery_mode] ||= 2
        properties = default_content_properties.merge(properties)
        body = serialize_and_encode_body(body, properties)
        result = conn.channel(1).basic_publish_confirm(body, exchange:, routing_key:, **properties)
        raise Error::PublishNotConfirmed unless result

        nil
      end
    end

    # Publish a (persistent) message but don't wait for a confirmation
    # Parameters are the same as Connection::Channel#basic_publish.
    # Options are the same as Connection::Channel#basic_publish.
    # Returns the same as Connection::Channel#basic_publish.
    # Raises the same as Connection::Channel#basic_publish.
    # Raises <tt>Error::UnsupportedContentType</tt> - If content type is unsupported
    # Raises <tt>Error::UnsupportedContentEncoding</tt> - If content encoding is unsupported
    def publish_and_forget(body, exchange:, routing_key: "", **properties)
      with_connection do |conn|
        properties[:delivery_mode] ||= 2
        properties = default_content_properties.merge(properties)
        body = serialize_and_encode_body(body, properties)
        conn.channel(1).basic_publish(body, exchange:, routing_key:, **properties)
      end
    end

    # Wait for unconfirmed publishes
    # Returns <tt>Boolean</tt> - True if successful, false if any message negatively acknowledged
    def wait_for_confirms
      with_connection do |conn|
        conn.channel(1).wait_for_confirms
      end
    end

    # :section: Queue actions

    # Consume messages from a queue
    # * <tt>queue</tt> (<tt>String</tt>) - Name of the queue to subscribe to
    # * <tt>no_ack</tt> (<tt>Boolean</tt>) - When false messages have to be manually acknowledged (or rejected) (default:
    #   false)
    # * <tt>prefetch</tt> (<tt>Integer</tt>) - Specify how many messages to prefetch for consumers with no_ack is false
    #   (default: 1)
    # * <tt>worker_threads</tt> (<tt>Integer</tt>) - Number of threads processing messages (default: 1)
    # * <tt>on_cancel</tt> (<tt>Proc</tt>) - Optional proc that will be called if the consumer is cancelled by the broker
    #   The proc will be called with the consumer tag as the only argument
    # * <tt>arguments</tt> (<tt>Hash</tt>) - Custom arguments to the consumer
    # Yields <tt>Message</tt> - Delivered message from the queue
    # Returns <tt>Consumer</tt> - The consumer object, which can be used to cancel the consumer
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
    # * <tt>queue</tt> (<tt>String</tt>) - Name of the queue to get the message from
    # * <tt>no_ack</tt> (<tt>Boolean</tt>) - When false the message has to be manually acknowledged (or rejected) (default:
    #   false)
    # Returns <tt>Message, nil</tt> - The message from the queue or nil if the queue is empty
    def get(queue, no_ack: false)
      with_connection do |conn|
        conn.with_channel do |ch|
          ch.basic_get(queue, no_ack:)
        end
      end
    end

    # Bind a queue to an exchange
    # * <tt>queue</tt> (<tt>String</tt>) - Name of the queue to bind
    # * <tt>exchange</tt> (<tt>String</tt>) - Name of the exchange to bind to
    # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key on which messages that match might be routed (depending on
    #   exchange type)
    # * <tt>arguments</tt> (<tt>Hash</tt>) - Message headers to match on (only relevant for header exchanges)
    # Returns <tt>nil</tt>.
    def bind(queue:, exchange:, binding_key: "", arguments: {})
      with_connection do |conn|
        conn.channel(1).queue_bind(queue, exchange:, binding_key:, arguments:)
      end
    end

    # Unbind a queue from an exchange
    # * <tt>queue</tt> (<tt>String</tt>) - Name of the queue to unbind
    # * <tt>exchange</tt> (<tt>String</tt>) - Name of the exchange to unbind from
    # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key which the queue is bound to the exchange with
    # * <tt>arguments</tt> (<tt>Hash</tt>) - Arguments matching the binding that's being removed
    # Returns <tt>nil</tt>.
    def unbind(queue:, exchange:, binding_key: "", arguments: {})
      with_connection do |conn|
        conn.channel(1).queue_unbind(queue, exchange:, binding_key:, arguments:)
      end
    end

    # Purge a queue
    # * <tt>queue</tt> (<tt>String</tt>) - Name of the queue
    # Returns <tt>nil</tt>.
    def purge(queue)
      with_connection do |conn|
        conn.channel(1).queue_purge(queue)
      end
    end

    # Delete a queue
    # * <tt>name</tt> (<tt>String</tt>) - Name of the queue
    # * <tt>if_unused</tt> (<tt>Boolean</tt>) - Only delete if the queue doesn't have consumers, raises a ChannelClosed
    #   error otherwise
    # * <tt>if_empty</tt> (<tt>Boolean</tt>) - Only delete if the queue is empty, raises a ChannelClosed error otherwise
    # Returns <tt>Integer</tt> - Number of messages in the queue when deleted
    def delete_queue(name, if_unused: false, if_empty: false)
      with_connection do |conn|
        msgs = conn.channel(1).queue_delete(name, if_unused:, if_empty:)
        @queues.delete(name)
        msgs
      end
    end

    # :section: Exchange actions

    # Bind an exchange to an exchange
    # * <tt>source</tt> (<tt>String</tt>) - Name of the exchange to bind to
    # * <tt>destination</tt> (<tt>String</tt>) - Name of the exchange to bind
    # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key on which messages that match might be routed (depending on
    #   exchange type)
    # * <tt>arguments</tt> (<tt>Hash</tt>) - Message headers to match on (only relevant for header exchanges)
    # Returns <tt>nil</tt>.
    def exchange_bind(source:, destination:, binding_key: "", arguments: {})
      with_connection do |conn|
        conn.channel(1).exchange_bind(destination:, source:, binding_key:, arguments:)
      end
    end

    # Unbind an exchange from an exchange
    # * <tt>source</tt> (<tt>String</tt>) - Name of the exchange to unbind from
    # * <tt>destination</tt> (<tt>String</tt>) - Name of the exchange to unbind
    # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key which the exchange is bound to the exchange with
    # * <tt>arguments</tt> (<tt>Hash</tt>) - Arguments matching the binding that's being removed
    # Returns <tt>nil</tt>.
    def exchange_unbind(source:, destination:, binding_key: "", arguments: {})
      with_connection do |conn|
        conn.channel(1).exchange_unbind(destination:, source:, binding_key:, arguments:)
      end
    end

    # Delete an exchange
    # * <tt>name</tt> (<tt>String</tt>) - Name of the exchange
    # Returns <tt>nil</tt>.
    def delete_exchange(name)
      with_connection do |conn|
        conn.channel(1).exchange_delete(name)
        @exchanges.delete(name)
        nil
      end
    end

    # :section: RPC

    # Create a RPC server for a single method/function/procedure
    # * <tt>method</tt> (<tt>String, Symbol</tt>) - name of the RPC method to host (i.e. queue name on the server side)
    # * <tt>worker_threads</tt> (<tt>Integer</tt>) - number of threads that process requests
    # * <tt>durable</tt> (<tt>Boolean</tt>) - If true the queue will survive broker restarts
    # * <tt>auto_delete</tt> (<tt>Boolean</tt>) - If true the queue will be deleted when the last consumer stops consuming
    #   (it won't be deleted until at least one consumer has consumed from it)
    # * <tt>arguments</tt> (<tt>Hash</tt>) - Custom arguments, such as queue-ttl etc.
    # Yields to the block - Block that processes the RPC request messages
    # Yields <tt>String</tt> - The body of the request message
    # The block should return <tt>String</tt> - The response message body
    # Returns the same as #subscribe.
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
    # * <tt>method</tt> (<tt>String, Symbol</tt>) - name of the RPC method to call (i.e. queue name on the server side)
    # * <tt>arguments</tt> (<tt>String</tt>) - arguments/body to the call
    # * <tt>timeout</tt> (<tt>Numeric, nil</tt>) - Number of seconds to wait for a response
    # Options are the same as Client#publish.
    # Returns <tt>String</tt> - Returns the result from the call
    # Raises <tt>Timeout::Error</tt> - if no response is received within the timeout period
    def rpc_call(method, arguments, timeout: nil, **properties)
      ch = with_connection(&:channel)
      begin
        msg = ch.basic_consume_once("amq.rabbitmq.reply-to", timeout:) do
          properties = default_content_properties.merge(properties)
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
    # Returns <tt>RPCClient</tt>.
    def rpc_client
      ch = with_connection(&:channel)
      RPCClient.new(ch).start
    end

    # :section: Message coding

    class << self
      # Configure the AMQP::Client class-level settings
      # Yields <tt>Configuration</tt> - Yields the configuration object for modification
      # Returns <tt>Configuration</tt> - The configuration object
      # === Example
      #   AMQP::Client.configure do |config|
      #     config.default_content_type = "application/json"
      #     config.strict_coding = true
      #   end
      def configure
        yield @config if block_given?
        @config
      end

      # Get the class-level configuration
      # Returns <tt>Configuration</tt>.
      attr_reader :config

      # Get the class-level codec registry
      # Returns <tt>MessageCodecRegistry</tt>.
      attr_reader :codec_registry

      # We need to set the subclass's configuration and codec registry
      # because these are class instance variables, hence not inherited.
      # Internal API.
      def inherited(subclass)
        super
        subclass_codec_registry = @codec_registry.dup
        subclass.instance_variable_set(:@codec_registry, subclass_codec_registry)
        subclass.instance_variable_set(:@config, Configuration.new(subclass_codec_registry))
        # Copy configuration settings from parent
        subclass.config.strict_coding = @config.strict_coding
        subclass.config.default_content_type = @config.default_content_type
        subclass.config.default_content_encoding = @config.default_content_encoding
      end
    end

    # Get the codec registry for this instance
    # Returns <tt>MessageCodecRegistry</tt>.
    attr_reader :codec_registry

    # Get/set if condig should be strict, i.e. if the client should raise on unknown codecs
    attr_accessor :strict_coding

    # Get/set the default content_type to use when publishing messages
    # Returns <tt>String, nil</tt>.
    attr_accessor :default_content_type

    # Get/set the default content_encoding to use when publishing messages
    # Returns <tt>String, nil</tt>.
    attr_accessor :default_content_encoding

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

    # Internal API.
    def cancel_consumer(consumer)
      @consumers.delete(consumer.id)
      with_connection do |conn|
        ch = conn.channel(consumer.channel_id)
        begin
          ch.basic_cancel(consumer.tag)
        ensure
          ch.close
        end
      end
    end

    private

    def parse_name(uri)
      return nil if uri.nil? || uri.empty?

      query = URI.parse(uri).query
      return nil unless query

      URI.decode_www_form(query).each { |k, v| return v if k == "name" }
      nil
    rescue URI::InvalidURIError
      nil
    end

    def log_lifecycle(level, event)
      return unless @logger

      @logger.public_send(level, "#{lifecycle_prefix}: #{event}")
    end

    def log_reconnect_error(err)
      if @logger
        @logger.warn("#{lifecycle_prefix}: reconnect error: #{err.inspect}")
      else
        warn "AMQP-Client reconnect error: #{err.inspect}"
      end
    end

    def thread_name(role)
      @name ? "amqp.#{role}[#{@name}]" : "amqp.#{role}"
    end

    def lifecycle_prefix
      @name ? "AMQP::Client[#{@name}]" : "AMQP::Client"
    end

    def default_content_properties
      {
        content_type: @default_content_type,
        content_encoding: @default_content_encoding
      }.compact
    end

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
