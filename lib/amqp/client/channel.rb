# frozen_string_literal: true

require_relative "message"
require "stringio"

module AMQP
  class Client
    class Connection
      # AMQP Channel
      class Channel
        # Should only be called from Connection
        # @param connection [Connection] The connection this channel belongs to
        # @param id [Integer] ID of the channel
        # @see Connection#channel
        # @api private
        def initialize(connection, id)
          @connection = connection
          @id = id
          @replies = ::Queue.new
          @consumers = {}
          @closed = nil
          @open = false
          @on_return = nil
          @confirm = nil
          @unconfirmed = []
          @unconfirmed_lock = Mutex.new
          @unconfirmed_empty = ConditionVariable.new
          @basic_gets = ::Queue.new
        end

        # Override #inspect
        # @api private
        def inspect
          "#<#{self.class} @id=#{@id} @open=#{@open} @closed=#{@closed} confirm_selected=#{!@confirm.nil?} " \
            "consumer_count=#{@consumers.size} replies_count=#{@replies.size} unconfirmed_count=#{@unconfirmed.size}>"
        end

        # Channel ID
        # @return [Integer]
        attr_reader :id

        # Open the channel (called from Connection)
        # @return [Channel] self
        # @api private
        def open
          return self if @open

          @open = true
          write_bytes FrameBytes.channel_open(@id)
          expect(:channel_open_ok)
          self
        end

        # Gracefully close channel
        # @param reason [String] The reason for closing the channel
        # @param code [Integer] The close code
        # @return [nil]
        def close(reason: "", code: 200)
          return if @closed

          write_bytes FrameBytes.channel_close(@id, reason, code)
          @closed = [:channel, code, reason]
          expect :channel_close_ok
          @replies.close
          @basic_gets.close
          @unconfirmed_lock.synchronize { @unconfirmed_empty.broadcast }
          @consumers.each_value { |c| close_consumer(c) }
          nil
        end

        # Called when channel is closed by broker
        # @param level [Symbol] :connection or :channel
        # @return [nil]
        # @api private
        def closed!(level, code, reason, classid, methodid)
          @closed = [level, code, reason, classid, methodid]
          @replies.close
          @basic_gets.close
          @unconfirmed_lock.synchronize { @unconfirmed_empty.broadcast }
          @consumers.each_value do |c|
            close_consumer(c)
            c.msg_q.clear # empty the queues too, messages can't be acked anymore
          end
          nil
        end

        # Handle returned messages in this block.  If not set the message will just be logged to STDERR
        # @yield [ReturnMessage] Messages returned by the broker when a publish has failed
        # @return nil
        def on_return(&block)
          @on_return = block
          nil
        end

        # @!group Exchange

        # Declare an exchange
        # @param name [String] Name of the exchange
        # @param type [String] Type of exchange (amq.direct, amq.fanout, amq.topic, amq.headers, etc.)
        # @param passive [Boolean] If true raise an exception if the exchange doesn't already exists
        # @param durable [Boolean] If true the exchange will persist between broker restarts,
        #   also a requirement for persistent messages
        # @param auto_delete [Boolean] If true the exchange will be deleted when the last queue/exchange is unbound
        # @param internal [Boolean] If true the exchange can't be published to directly
        # @param arguments [Hash] Custom arguments
        # @return [nil]
        def exchange_declare(name, type:, passive: false, durable: true, auto_delete: false, internal: false, arguments: {})
          write_bytes FrameBytes.exchange_declare(@id, name, type, passive, durable, auto_delete, internal, arguments)
          expect :exchange_declare_ok
          nil
        end

        # Delete an exchange
        # @param name [String] Name of the exchange
        # @param if_unused [Boolean] If true raise an exception if queues/exchanges is bound to this exchange
        # @param no_wait [Boolean] If true don't wait for a broker confirmation
        # @return [nil]
        def exchange_delete(name, if_unused: false, no_wait: false)
          write_bytes FrameBytes.exchange_delete(@id, name, if_unused, no_wait)
          expect :exchange_delete_ok unless no_wait
          nil
        end

        # Bind an exchange to another exchange
        # @param source [String] Name of the source exchange
        # @param destination [String] Name of the destination exchange
        # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
        # @param arguments [Hash] Message headers to match on, but only when bound to header exchanges
        # @return [nil]
        def exchange_bind(source:, destination:, binding_key:, arguments: {})
          write_bytes FrameBytes.exchange_bind(@id, destination, source, binding_key, false, arguments)
          expect :exchange_bind_ok
          nil
        end

        # Unbind an exchange from another exchange
        # @param source [String] Name of the source exchange
        # @param destination [String] Name of the destination exchange
        # @param binding_key [String] Binding key which the queue is bound to the exchange with
        # @param arguments [Hash] Arguments matching the binding that's being removed
        # @return [nil]
        def exchange_unbind(source:, destination:, binding_key:, arguments: {})
          write_bytes FrameBytes.exchange_unbind(@id, destination, source, binding_key, false, arguments)
          expect :exchange_unbind_ok
          nil
        end

        # @!endgroup
        # @!group Queue

        # Response when declaring a Queue
        # @!attribute queue_name
        #   @return [String] The name of the queue
        # @!attribute message_count
        #   @return [Integer] Number of messages in the queue at the time of declaration
        # @!attribute consumer_count
        #   @return [Integer] Number of consumers subscribed to the queue at the time of declaration
        QueueOk = Data.define(:queue_name, :message_count, :consumer_count)

        # Create a queue (operation is idempotent)
        # @param name [String] Name of the queue, can be empty, but will then be generated by the broker
        # @param passive [Boolean] If true an exception will be raised if the queue doesn't already exists
        # @param durable [Boolean] If true the queue will survive broker restarts,
        #   messages in the queue will only survive if they are published as persistent
        # @param exclusive [Boolean] If true the queue will be deleted when the channel is closed
        # @param auto_delete [Boolean] If true the queue will be deleted when the last consumer stops consuming
        #   (it won't be deleted until at least one consumer has consumed from it)
        # @param arguments [Hash] Custom arguments, such as queue-ttl etc.
        # @return [QueueOk]
        def queue_declare(name = "", passive: false, durable: true, exclusive: false, auto_delete: false, arguments: {})
          durable = false if name.empty?
          exclusive = true if name.empty?
          auto_delete = true if name.empty?

          write_bytes FrameBytes.queue_declare(@id, name, passive, durable, exclusive, auto_delete, arguments)
          name, message_count, consumer_count = expect(:queue_declare_ok)

          QueueOk.new(name, message_count, consumer_count)
        end

        # Delete a queue
        # @param name [String] Name of the queue
        # @param if_unused [Boolean] Only delete if the queue doesn't have consumers, raises a ChannelClosed error otherwise
        # @param if_empty [Boolean] Only delete if the queue is empty, raises a ChannelClosed error otherwise
        # @param no_wait [Boolean] Don't wait for a broker confirmation if true
        # @return [Integer] Number of messages in queue when deleted
        # @return [nil] If no_wait was set true
        def queue_delete(name, if_unused: false, if_empty: false, no_wait: false)
          write_bytes FrameBytes.queue_delete(@id, name, if_unused, if_empty, no_wait)
          message_count, = expect :queue_delete unless no_wait
          message_count
        end

        # Bind a queue to an exchange
        # @param name [String] Name of the queue
        # @param exchange [String] Name of the exchange
        # @param binding_key [String] Binding key on which messages that match might be routed (depending on exchange type)
        # @param arguments [Hash] Message headers to match on, but only when bound to header exchanges
        # @return [nil]
        def queue_bind(name, exchange:, binding_key: "", arguments: {})
          write_bytes FrameBytes.queue_bind(@id, name, exchange, binding_key, false, arguments)
          expect :queue_bind_ok
          nil
        end

        # Purge a queue
        # @param name [String] Name of the queue
        # @param no_wait [Boolean] Don't wait for a broker confirmation if true
        # @return [Integer] Number of messages in queue when purged
        # @return [nil] If no_wait was set true
        def queue_purge(name, no_wait: false)
          write_bytes FrameBytes.queue_purge(@id, name, no_wait)
          message_count, = expect :queue_purge_ok unless no_wait
          message_count
        end

        # Unbind a queue from an exchange
        # @param name [String] Name of the queue
        # @param exchange [String] Name of the exchange
        # @param binding_key [String] Binding key which the queue is bound to the exchange with
        # @param arguments [Hash] Arguments matching the binding that's being removed
        # @return [nil]
        def queue_unbind(name, exchange:, binding_key: "", arguments: {})
          write_bytes FrameBytes.queue_unbind(@id, name, exchange, binding_key, arguments)
          expect :queue_unbind_ok
          nil
        end

        # @!endgroup
        # @!group Basic

        # Get a message from a queue (by polling)
        # @param queue_name [String]
        # @param no_ack [Boolean] When false the message have to be manually acknowledged
        # @return [Message] If the queue had a message
        # @return [nil] If the queue doesn't have any messages
        def basic_get(queue_name, no_ack: true)
          write_bytes FrameBytes.basic_get(@id, queue_name, no_ack)
          case (msg = @basic_gets.pop)
          when Message then msg
          when :basic_get_empty then nil
          when nil              then raise Error::Closed.new(@id, *@closed)
          end
        end

        # Publishes a message to an exchange
        # @param body [String] The body
        # @param exchange [String] Name of the exchange to publish to
        # @param routing_key [String] The routing key that the exchange might use to route the message to a queue
        # @param properties [Properties]
        # @option properties [Boolean] mandatory The message will be returned if the message can't be routed to a queue
        # @option properties [Boolean] persistent Same as delivery_mode: 2
        # @option properties [String] content_type Content type of the message body
        # @option properties [String] content_encoding Content encoding of the body
        # @option properties [Hash<String, Object>] headers Custom headers
        # @option properties [Integer] delivery_mode 2 for persisted message, transient messages for all other values
        # @option properties [Integer] priority A priority of the message (between 0 and 255)
        # @option properties [String] correlation_id A correlation id, most often used used for RPC communication
        # @option properties [String] reply_to Queue to reply RPC responses to
        # @option properties [Integer, String] expiration Number of seconds the message will stay in the queue
        # @option properties [String] message_id Can be used to uniquely identify the message, e.g. for deduplication
        # @option properties [Date] timestamp Often used for the time the message was originally generated
        # @option properties [String] type Can indicate what kind of message this is
        # @option properties [String] user_id Can be used to verify that this is the user that published the message
        # @option properties [String] app_id Can be used to indicates which app that generated the message
        # @return [nil]
        def basic_publish(body, exchange:, routing_key: "", **properties)
          body_max = @connection.frame_max - 8
          id = @id
          mandatory = properties.delete(:mandatory) || false
          case properties.delete(:persistent)
          when true then properties[:delivery_mode] = 2
          when false then properties[:delivery_mode] = 1
          end
          if @confirm
            @unconfirmed_lock.synchronize do
              @unconfirmed.push @confirm += 1
            end
          end
          if body.bytesize.between?(1, body_max)
            write_bytes FrameBytes.basic_publish(id, exchange, routing_key, mandatory),
                        FrameBytes.header(id, body.bytesize, properties),
                        FrameBytes.body(id, body)
            return
          end

          write_bytes FrameBytes.basic_publish(id, exchange, routing_key, mandatory),
                      FrameBytes.header(id, body.bytesize, properties)
          pos = 0
          while pos < body.bytesize # split body into multiple frame_max frames
            len = [body_max, body.bytesize - pos].min
            body_part = body.byteslice(pos, len)
            write_bytes FrameBytes.body(id, body_part)
            pos += len
          end
          nil
        end

        # Publish a message and block until the message has confirmed it has received it
        # @param (see #basic_publish)
        # @option (see #basic_publish)
        # @return [Boolean] True if the message was successfully published
        # @raise (see #basic_publish)
        def basic_publish_confirm(body, exchange:, routing_key: "", **properties)
          confirm_select(no_wait: true)
          basic_publish(body, exchange:, routing_key:, **properties)
          wait_for_confirms
        end

        # Response when subscribing (starting a consumer)
        # @!attribute channel_id
        #   @return [Integer] The channel ID
        # @!attribute consumer_tag
        #   @return [String] The consumer tag
        # @!attribute worker_threads
        #   @return [Array<Thread>] Array of worker threads
        ConsumeOk = Data.define(:channel_id, :consumer_tag, :worker_threads, :msg_q, :on_cancel)

        # Consume messages from a queue
        # @param queue [String] Name of the queue to subscribe to
        # @param tag [String] Custom consumer tag, will be auto assigned by the broker if empty.
        #   Has to be unique among this channel's consumers only
        # @param no_ack [Boolean] When false messages have to be manually acknowledged (or rejected)
        # @param exclusive [Boolean] When true only a single consumer can consume from the queue at a time
        # @param arguments [Hash] Custom arguments for the consumer
        # @param worker_threads [Integer] Number of threads processing messages,
        #   0 means that the thread calling this method will process the messages and thus this method will block
        # @param on_cancel [Proc] Optional proc that will be called if the consumer is cancelled by the broker
        #   The proc will be called with the consumer tag as the only argument
        # @yield [Message] Delivered message from the queue
        # @return [ConsumeOk]
        # @return [nil] When `worker_threads` is 0 the method will return when the consumer is cancelled
        def basic_consume(queue, tag: "", no_ack: true, exclusive: false, no_wait: false,
                          arguments: {}, worker_threads: 1, on_cancel: nil, &blk)
          raise ArgumentError, "consumer_tag required when no_wait" if no_wait && tag.empty?

          write_bytes FrameBytes.basic_consume(@id, queue, tag, no_ack, exclusive, no_wait, arguments)
          consumer_tag, = expect(:basic_consume_ok) unless no_wait
          msg_q = ::Queue.new
          if worker_threads.zero?
            @consumers[consumer_tag] =
              ConsumeOk.new(channel_id: @id, consumer_tag:, worker_threads: [], msg_q:, on_cancel:)
            consume_loop(msg_q, consumer_tag, &blk)
            nil
          else
            threads = Array.new(worker_threads) do
              Thread.new { consume_loop(msg_q, consumer_tag, &blk) }
            end
            @consumers[consumer_tag] =
              ConsumeOk.new(channel_id: @id, consumer_tag:, worker_threads: threads, msg_q:, on_cancel:)
          end
        end

        # Consume a single message from a queue
        # @param queue [String] Name of the queue to subscribe to
        # @param timeout [Numeric, nil] Number of seconds to wait for a message
        # @yield [] Block in which the message will be yielded
        # @return [Message] The single message received from the queue
        # @raise [Timeout::Error] if no response is received within the timeout period
        def basic_consume_once(queue, timeout: nil, &)
          tag = "consume-once-#{rand(1024)}"
          write_bytes FrameBytes.basic_consume(@id, queue, tag, true, false, true, nil)
          msg_q = ::Queue.new
          @consumers[tag] =
            ConsumeOk.new(channel_id: @id, consumer_tag: tag, worker_threads: [], msg_q:, on_cancel: nil)
          yield if block_given?
          msg = msg_q.pop(timeout:)
          write_bytes FrameBytes.basic_cancel(@id, tag, no_wait: true)
          consumer = @consumers.delete(tag)
          close_consumer(consumer)
          raise Timeout::Error, "No message received in #{timeout} seconds" if timeout && msg.nil?

          msg
        end

        # Cancel/abort/stop a consumer
        # @param consumer_tag [String] Tag of the consumer to cancel
        # @param no_wait [Boolean] Will wait for a confirmation from the broker that the consumer is cancelled
        # @return [nil]
        def basic_cancel(consumer_tag, no_wait: false)
          consumer = @consumers[consumer_tag]
          return unless consumer

          write_bytes FrameBytes.basic_cancel(@id, consumer_tag)
          expect(:basic_cancel_ok) unless no_wait
          @consumers.delete(consumer_tag)
          close_consumer(consumer)
          nil
        end

        # Specify how many messages to prefetch for consumers with `no_ack: false`
        # @param prefetch_count [Integer] Number of messages to maximum keep in flight
        # @param prefetch_size [Integer] Number of bytes to maximum keep in flight
        # @param global [Boolean] If true the limit will apply to channel rather than the consumer
        # @return [nil]
        def basic_qos(prefetch_count, prefetch_size: 0, global: false)
          write_bytes FrameBytes.basic_qos(@id, prefetch_size, prefetch_count, global)
          expect :basic_qos_ok
          nil
        end

        # Acknowledge a message
        # @param delivery_tag [Integer] The delivery tag of the message to acknowledge
        # @param multiple [Boolean] Ack all messages up to this message
        # @return [nil]
        def basic_ack(delivery_tag, multiple: false)
          write_bytes FrameBytes.basic_ack(@id, delivery_tag, multiple)
          nil
        end

        # Negatively acknowledge a message
        # @param delivery_tag [Integer] The delivery tag of the message to acknowledge
        # @param multiple [Boolean] Nack all messages up to this message
        # @param requeue [Boolean] Requeue the message
        # @return [nil]
        def basic_nack(delivery_tag, multiple: false, requeue: false)
          write_bytes FrameBytes.basic_nack(@id, delivery_tag, multiple, requeue)
          nil
        end

        # Reject a message
        # @param delivery_tag [Integer] The delivery tag of the message to acknowledge
        # @param requeue [Boolean] Requeue the message into the queue again
        # @return [nil]
        def basic_reject(delivery_tag, requeue: false)
          write_bytes FrameBytes.basic_reject(@id, delivery_tag, requeue)
          nil
        end

        # Recover all the unacknowledge messages
        # @param requeue [Boolean] If false the currently unack:ed messages will be deliviered to this consumer again,
        #   if true to any consumer
        # @return [nil]
        def basic_recover(requeue: false)
          write_bytes FrameBytes.basic_recover(@id, requeue:)
          expect :basic_recover_ok
          nil
        end

        # @!endgroup
        # @!group Confirm

        # Put the channel in confirm mode, each published message will then be confirmed by the broker
        # @param no_wait [Boolean] If false the method will block until the broker has confirmed the request
        # @return [nil]
        def confirm_select(no_wait: false)
          return if @confirm # fast path

          @unconfirmed_lock.synchronize do
            # check again in case another thread already did this while we waited for the lock
            return if @confirm

            write_bytes FrameBytes.confirm_select(@id, no_wait)
            expect :confirm_select_ok unless no_wait
            @confirm = 0
          end
          nil
        end

        # Block until all publishes messages are confirmed
        # @return nil
        def wait_for_confirms
          @unconfirmed_lock.synchronize do
            until @unconfirmed.empty?
              @unconfirmed_empty.wait(@unconfirmed_lock)
              raise Error::Closed.new(@id, *@closed) if @closed
            end
          end
        end

        # Called by Connection when received ack/nack from broker
        # @api private
        def confirm(args)
          _ack_or_nack, delivery_tag, multiple = *args
          @unconfirmed_lock.synchronize do
            case multiple
            when true
              idx = @unconfirmed.index(delivery_tag) || raise("Delivery tag not found")
              @unconfirmed.shift(idx + 1)
            when false
              @unconfirmed.delete(delivery_tag) || raise("Delivery tag not found")
            end
            @unconfirmed_empty.broadcast if @unconfirmed.empty?
          end
        end

        # @!endgroup
        # @!group Transaction

        # Put the channel in transaction mode, make sure that you #tx_commit or #tx_rollback after publish
        # @return [nil]
        def tx_select
          write_bytes FrameBytes.tx_select(@id)
          expect :tx_select_ok
          nil
        end

        # Commmit a transaction, requires that the channel is in transaction mode
        # @return [nil]
        def tx_commit
          write_bytes FrameBytes.tx_commit(@id)
          expect :tx_commit_ok
          nil
        end

        # Rollback a transaction, requires that the channel is in transaction mode
        # @return [nil]
        def tx_rollback
          write_bytes FrameBytes.tx_rollback(@id)
          expect :tx_rollback_ok
          nil
        end

        # @!endgroup

        # @api private
        def reply(args)
          @replies.push(args)
        end

        # @api private
        def message_returned(reply_code, reply_text, exchange, routing_key)
          @next_msg = ReturnMessage.new(reply_code, reply_text, exchange, routing_key)
        end

        # @api private
        def message_delivered(consumer_tag, delivery_tag, redelivered, exchange, routing_key)
          @next_msg = Message.new(self, consumer_tag, delivery_tag, exchange, routing_key, redelivered)
        end

        # @api private
        def basic_get_empty
          @basic_gets.push :basic_get_empty
        end

        # @api private
        def header_delivered(body_size, properties)
          @next_msg.properties = properties
          if body_size.zero?
            next_message_finished!
          else
            @next_body = StringIO.new(String.new(capacity: body_size))
            @next_body_size = body_size
          end
        end

        # @api private
        def body_delivered(body_part)
          @next_body.write(body_part)
          return unless @next_body.pos == @next_body_size

          @next_msg.body = @next_body.string
          next_message_finished!
        end

        # Handle consumer cancellation from the broker
        # @api private
        def cancel_consumer(tag)
          consumer = @consumers.delete(tag)
          return unless consumer

          close_consumer(consumer)
          begin
            consumer.on_cancel&.call(consumer.consumer_tag)
          rescue StandardError => e
            warn "AMQP-Client consumer on_cancel callback error: #{e.class}: #{e.message}"
          end
          nil
        end

        private

        def close_consumer(consumer)
          consumer.msg_q.close
          # The worker threads will exit when the queue is closed
          nil
        end

        def next_message_finished!
          next_msg = @next_msg
          if next_msg.is_a? ReturnMessage
            if @on_return
              Thread.new { @on_return.call(next_msg) }
            else
              warn "AMQP-Client message returned: #{next_msg.inspect}"
            end
          elsif next_msg.consumer_tag.nil?
            @basic_gets.push next_msg
          else
            Thread.pass until (consumer = @consumers[next_msg.consumer_tag])
            consumer.msg_q.push next_msg
          end
          nil
        ensure
          @next_msg = @next_body = @next_body_size = nil
        end

        def write_bytes(*bytes)
          raise Error::Closed.new(@id, *@closed) if @closed

          @connection.write_bytes(*bytes)
        end

        def expect(expected_frame_type)
          frame_type, *args = @replies.pop
          raise Error::Closed.new(@id, *@closed) if frame_type.nil?
          raise Error::UnexpectedFrameType.new(expected_frame_type, frame_type) unless frame_type == expected_frame_type

          args
        end

        def consume_loop(queue, tag)
          while (msg = queue.pop)
            begin
              yield msg
            rescue StandardError # cancel the consumer if an uncaught exception is raised
              begin
                close("Unexpected exception in consumer #{tag} thread", 500)
              rescue StandardError # ignore sockets errors while canceling
                nil
              end
              raise # reraise original exception
            end
          end
        end
      end
    end
  end
end
