# frozen_string_literal: true

require_relative "message"
require "stringio"

module AMQP
  class Client
    class Connection
      # AMQP Channel
      class Channel
        # Should only be called from Connection
        # * <tt>connection</tt> (<tt>Connection</tt>) - The connection this channel belongs to
        # * <tt>id</tt> (<tt>Integer</tt>) - ID of the channel
        # See Connection#channel.
        # Internal API.
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
          @nacked = false
          @basic_gets = ::Queue.new
        end

        # Override #inspect
        # Internal API.
        def inspect
          "#<#{self.class} @id=#{@id} @open=#{@open} @closed=#{@closed} confirm_selected=#{!@confirm.nil?} " \
            "consumer_count=#{@consumers.size} replies_count=#{@replies.size} unconfirmed_count=#{@unconfirmed.size}>"
        end

        # Channel ID
        # Returns <tt>Integer</tt>.
        attr_reader :id

        # Connection this channel belongs to
        # Returns <tt>Connection</tt>.
        attr_reader :connection

        # Open the channel (called from Connection)
        # Returns <tt>Channel</tt> - self
        # Internal API.
        def open
          return self if @open

          @open = true
          write_bytes FrameBytes.channel_open(@id)
          expect(:channel_open_ok)
          self
        end

        # Gracefully close channel
        # * <tt>reason</tt> (<tt>String</tt>) - The reason for closing the channel
        # * <tt>code</tt> (<tt>Integer</tt>) - The close code
        # Returns <tt>nil</tt>.
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
        # * <tt>level</tt> (<tt>Symbol</tt>) - :connection or :channel
        # Returns <tt>nil</tt>.
        # Internal API.
        def closed!(level, code, reason, classid, methodid)
          return if @closed

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
        # Yields <tt>ReturnMessage</tt> - Messages returned by the broker when a publish has failed
        # Returns <tt>nil</tt>.
        def on_return(&block)
          @on_return = block
          nil
        end

        # :section: Exchange

        # Declare an exchange
        # * <tt>name</tt> (<tt>String</tt>) - Name of the exchange
        # * <tt>type</tt> (<tt>String</tt>) - Type of exchange (amq.direct, amq.fanout, amq.topic, amq.headers, etc.)
        # * <tt>passive</tt> (<tt>Boolean</tt>) - If true raise an exception if the exchange doesn't already exists
        # * <tt>durable</tt> (<tt>Boolean</tt>) - If true the exchange will persist between broker restarts,
        #   also a requirement for persistent messages
        # * <tt>auto_delete</tt> (<tt>Boolean</tt>) - If true the exchange will be deleted when the last queue/exchange is
        #   unbound
        # * <tt>internal</tt> (<tt>Boolean</tt>) - If true the exchange can't be published to directly
        # * <tt>arguments</tt> (<tt>Hash</tt>) - Custom arguments
        # Returns <tt>nil</tt>.
        def exchange_declare(name, type:, passive: false, durable: true, auto_delete: false, internal: false, arguments: {})
          write_bytes FrameBytes.exchange_declare(@id, name, type, passive, durable, auto_delete, internal, arguments)
          expect :exchange_declare_ok
          nil
        end

        # Delete an exchange
        # * <tt>name</tt> (<tt>String</tt>) - Name of the exchange
        # * <tt>if_unused</tt> (<tt>Boolean</tt>) - If true raise an exception if queues/exchanges is bound to this exchange
        # * <tt>no_wait</tt> (<tt>Boolean</tt>) - If true don't wait for a broker confirmation
        # Returns <tt>nil</tt>.
        def exchange_delete(name, if_unused: false, no_wait: false)
          write_bytes FrameBytes.exchange_delete(@id, name, if_unused, no_wait)
          expect :exchange_delete_ok unless no_wait
          nil
        end

        # Bind an exchange to another exchange
        # * <tt>source</tt> (<tt>String</tt>) - Name of the source exchange
        # * <tt>destination</tt> (<tt>String</tt>) - Name of the destination exchange
        # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key on which messages that match might be routed (depending on
        #   exchange type)
        # * <tt>arguments</tt> (<tt>Hash</tt>) - Message headers to match on, but only when bound to header exchanges
        # Returns <tt>nil</tt>.
        def exchange_bind(source:, destination:, binding_key:, arguments: {})
          write_bytes FrameBytes.exchange_bind(@id, destination, source, binding_key, false, arguments)
          expect :exchange_bind_ok
          nil
        end

        # Unbind an exchange from another exchange
        # * <tt>source</tt> (<tt>String</tt>) - Name of the source exchange
        # * <tt>destination</tt> (<tt>String</tt>) - Name of the destination exchange
        # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key which the queue is bound to the exchange with
        # * <tt>arguments</tt> (<tt>Hash</tt>) - Arguments matching the binding that's being removed
        # Returns <tt>nil</tt>.
        def exchange_unbind(source:, destination:, binding_key:, arguments: {})
          write_bytes FrameBytes.exchange_unbind(@id, destination, source, binding_key, false, arguments)
          expect :exchange_unbind_ok
          nil
        end

        # :section: Queue

        # Response when declaring a Queue
        # Attribute: <tt>queue_name</tt>
        # Returns <tt>String</tt> - The name of the queue
        # Attribute: <tt>message_count</tt>
        # Returns <tt>Integer</tt> - Number of messages in the queue at the time of declaration
        # Attribute: <tt>consumer_count</tt>
        # Returns <tt>Integer</tt> - Number of consumers subscribed to the queue at the time of declaration
        QueueOk = Data.define(:queue_name, :message_count, :consumer_count)

        # Create a queue (operation is idempotent)
        # * <tt>name</tt> (<tt>String</tt>) - Name of the queue, can be empty, but will then be generated by the broker
        # * <tt>passive</tt> (<tt>Boolean</tt>) - If true an exception will be raised if the queue doesn't already exists
        # * <tt>durable</tt> (<tt>Boolean</tt>) - If true the queue will survive broker restarts,
        #   messages in the queue will only survive if they are published as persistent
        # * <tt>exclusive</tt> (<tt>Boolean</tt>) - If true the queue will be deleted when the connection is closed
        # * <tt>auto_delete</tt> (<tt>Boolean</tt>) - If true the queue will be deleted when the last consumer stops
        #   consuming
        #   (it won't be deleted until at least one consumer has consumed from it)
        # * <tt>arguments</tt> (<tt>Hash</tt>) - Custom arguments, such as queue-ttl etc.
        # Returns <tt>QueueOk</tt>.
        def queue_declare(name = "", passive: false, durable: true, exclusive: false, auto_delete: false, arguments: {})
          durable = false if name.empty?
          exclusive = true if name.empty?
          auto_delete = true if name.empty?

          write_bytes FrameBytes.queue_declare(@id, name, passive, durable, exclusive, auto_delete, arguments)
          name, message_count, consumer_count = expect(:queue_declare_ok)

          QueueOk.new(name, message_count, consumer_count)
        end

        # Delete a queue
        # * <tt>name</tt> (<tt>String</tt>) - Name of the queue
        # * <tt>if_unused</tt> (<tt>Boolean</tt>) - Only delete if the queue doesn't have consumers, raises a ChannelClosed
        #   error otherwise
        # * <tt>if_empty</tt> (<tt>Boolean</tt>) - Only delete if the queue is empty, raises a ChannelClosed error otherwise
        # * <tt>no_wait</tt> (<tt>Boolean</tt>) - Don't wait for a broker confirmation if true
        # Returns <tt>Integer</tt> - Number of messages in queue when deleted
        # Returns <tt>nil</tt> - If no_wait was set true
        def queue_delete(name, if_unused: false, if_empty: false, no_wait: false)
          write_bytes FrameBytes.queue_delete(@id, name, if_unused, if_empty, no_wait)
          message_count, = expect :queue_delete unless no_wait
          message_count
        end

        # Bind a queue to an exchange
        # * <tt>name</tt> (<tt>String</tt>) - Name of the queue
        # * <tt>exchange</tt> (<tt>String</tt>) - Name of the exchange
        # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key on which messages that match might be routed (depending on
        #   exchange type)
        # * <tt>arguments</tt> (<tt>Hash</tt>) - Message headers to match on, but only when bound to header exchanges
        # Returns <tt>nil</tt>.
        def queue_bind(name, exchange:, binding_key: "", arguments: {})
          write_bytes FrameBytes.queue_bind(@id, name, exchange, binding_key, false, arguments)
          expect :queue_bind_ok
          nil
        end

        # Purge a queue
        # * <tt>name</tt> (<tt>String</tt>) - Name of the queue
        # * <tt>no_wait</tt> (<tt>Boolean</tt>) - Don't wait for a broker confirmation if true
        # Returns <tt>Integer</tt> - Number of messages in queue when purged
        # Returns <tt>nil</tt> - If no_wait was set true
        def queue_purge(name, no_wait: false)
          write_bytes FrameBytes.queue_purge(@id, name, no_wait)
          message_count, = expect :queue_purge_ok unless no_wait
          message_count
        end

        # Unbind a queue from an exchange
        # * <tt>name</tt> (<tt>String</tt>) - Name of the queue
        # * <tt>exchange</tt> (<tt>String</tt>) - Name of the exchange
        # * <tt>binding_key</tt> (<tt>String</tt>) - Binding key which the queue is bound to the exchange with
        # * <tt>arguments</tt> (<tt>Hash</tt>) - Arguments matching the binding that's being removed
        # Returns <tt>nil</tt>.
        def queue_unbind(name, exchange:, binding_key: "", arguments: {})
          write_bytes FrameBytes.queue_unbind(@id, name, exchange, binding_key, arguments)
          expect :queue_unbind_ok
          nil
        end

        # :section: Basic

        # Get a message from a queue (by polling)
        # * <tt>queue_name</tt> (<tt>String</tt>) -
        # * <tt>no_ack</tt> (<tt>Boolean</tt>) - When false the message have to be manually acknowledged
        # Returns <tt>Message</tt> - If the queue had a message
        # Returns <tt>nil</tt> - If the queue doesn't have any messages
        def basic_get(queue_name, no_ack: true)
          write_bytes FrameBytes.basic_get(@id, queue_name, no_ack)
          case (msg = @basic_gets.pop)
          when Message then msg
          when :basic_get_empty then nil
          when nil              then raise Error::Closed.new(@id, *@closed)
          end
        end

        # Publishes a message to an exchange
        # * <tt>body</tt> (<tt>String</tt>) - The body
        # * <tt>exchange</tt> (<tt>String</tt>) - Name of the exchange to publish to
        # * <tt>routing_key</tt> (<tt>String</tt>) - The routing key that the exchange might use to route the message to a
        #   queue
        # * <tt>properties</tt> (<tt>Properties</tt>) -
        # * <tt>mandatory</tt> (<tt>Boolean</tt>) - The message will be returned if the message can't be routed to a queue
        # * <tt>persistent</tt> (<tt>Boolean</tt>) - Same as delivery_mode: 2
        # * <tt>content_type</tt> (<tt>String</tt>) - Content type of the message body
        # * <tt>content_encoding</tt> (<tt>String</tt>) - Content encoding of the body
        # * <tt>headers</tt> (<tt>Hash<String, Object></tt>) - Custom headers
        # * <tt>delivery_mode</tt> (<tt>Integer</tt>) - 2 for persisted message, transient messages for all other values
        # * <tt>priority</tt> (<tt>Integer</tt>) - A priority of the message (between 0 and 255)
        # * <tt>correlation_id</tt> (<tt>String</tt>) - A correlation id, most often used used for RPC communication
        # * <tt>reply_to</tt> (<tt>String</tt>) - Queue to reply RPC responses to
        # * <tt>expiration</tt> (<tt>Integer, String</tt>) - Number of seconds the message will stay in the queue
        # * <tt>message_id</tt> (<tt>String</tt>) - Can be used to uniquely identify the message, e.g. for deduplication
        # * <tt>timestamp</tt> (<tt>Date</tt>) - Often used for the time the message was originally generated
        # * <tt>type</tt> (<tt>String</tt>) - Can indicate what kind of message this is
        # * <tt>user_id</tt> (<tt>String</tt>) - Can be used to verify that this is the user that published the message
        # * <tt>app_id</tt> (<tt>String</tt>) - Can be used to indicates which app that generated the message
        # Returns <tt>nil</tt>.
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
        # Parameters are the same as #basic_publish.
        # Options are the same as #basic_publish.
        # Returns <tt>Boolean</tt> - True if the message was successfully published
        # Raises the same as #basic_publish.
        def basic_publish_confirm(body, exchange:, routing_key: "", **properties)
          confirm_select(no_wait: true)
          basic_publish(body, exchange:, routing_key:, **properties)
          wait_for_confirms
        end

        # Response when subscribing (starting a consumer)
        # Attribute: <tt>channel_id</tt>
        # Returns <tt>Integer</tt> - The channel ID
        # Attribute: <tt>consumer_tag</tt>
        # Returns <tt>String</tt> - The consumer tag
        # Attribute: <tt>worker_threads</tt>
        # Returns <tt>Array<Thread></tt> - Array of worker threads
        ConsumeOk = Data.define(:channel_id, :consumer_tag, :worker_threads, :msg_q, :on_cancel)

        # Consume messages from a queue
        # * <tt>queue</tt> (<tt>String</tt>) - Name of the queue to subscribe to
        # * <tt>tag</tt> (<tt>String</tt>) - Custom consumer tag, will be auto assigned by the broker if empty.
        #   Has to be unique among this channel's consumers only
        # * <tt>no_ack</tt> (<tt>Boolean</tt>) - When false messages have to be manually acknowledged (or rejected)
        # * <tt>exclusive</tt> (<tt>Boolean</tt>) - When true only a single consumer can consume from the queue at a time
        # * <tt>arguments</tt> (<tt>Hash</tt>) - Custom arguments for the consumer
        # * <tt>worker_threads</tt> (<tt>Integer</tt>) - Number of threads processing messages,
        #   0 means that the thread calling this method will process the messages and thus this method will block
        # * <tt>on_cancel</tt> (<tt>Proc</tt>) - Optional proc that will be called if the consumer is cancelled by the
        #   broker
        #   The proc will be called with the consumer tag as the only argument
        # Yields <tt>Message</tt> - Delivered message from the queue
        # Returns <tt>ConsumeOk</tt>.
        # Returns <tt>nil</tt> - When `worker_threads` is 0 the method will return when the consumer is cancelled
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
            threads = Array.new(worker_threads) do |i|
              t = Thread.new { consume_loop(msg_q, consumer_tag, &blk) }
              t.name = @connection.thread_name(role: "consumer", detail: "ch=#{@id} tag=#{consumer_tag} ##{i + 1}")
              t
            end
            @consumers[consumer_tag] =
              ConsumeOk.new(channel_id: @id, consumer_tag:, worker_threads: threads, msg_q:, on_cancel:)
          end
        end

        # Consume a single message from a queue
        # * <tt>queue</tt> (<tt>String</tt>) - Name of the queue to subscribe to
        # * <tt>timeout</tt> (<tt>Numeric, nil</tt>) - Number of seconds to wait for a message
        # Yields to the block - Block in which the message will be yielded
        # Returns <tt>Message</tt> - The single message received from the queue
        # Raises <tt>Timeout::Error</tt> - if no response is received within the timeout period
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
        # * <tt>consumer_tag</tt> (<tt>String</tt>) - Tag of the consumer to cancel
        # * <tt>no_wait</tt> (<tt>Boolean</tt>) - Will wait for a confirmation from the broker that the consumer is
        #   cancelled
        # Returns <tt>nil</tt>.
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
        # * <tt>prefetch_count</tt> (<tt>Integer</tt>) - Number of messages to maximum keep in flight
        # * <tt>prefetch_size</tt> (<tt>Integer</tt>) - Number of bytes to maximum keep in flight
        # * <tt>global</tt> (<tt>Boolean</tt>) - If true the limit will apply to channel rather than the consumer
        # Returns <tt>nil</tt>.
        def basic_qos(prefetch_count, prefetch_size: 0, global: false)
          write_bytes FrameBytes.basic_qos(@id, prefetch_size, prefetch_count, global)
          expect :basic_qos_ok
          nil
        end

        # Acknowledge a message
        # * <tt>delivery_tag</tt> (<tt>Integer</tt>) - The delivery tag of the message to acknowledge
        # * <tt>multiple</tt> (<tt>Boolean</tt>) - Ack all messages up to this message
        # Returns <tt>nil</tt>.
        def basic_ack(delivery_tag, multiple: false)
          write_bytes FrameBytes.basic_ack(@id, delivery_tag, multiple)
          nil
        end

        # Negatively acknowledge a message
        # * <tt>delivery_tag</tt> (<tt>Integer</tt>) - The delivery tag of the message to acknowledge
        # * <tt>multiple</tt> (<tt>Boolean</tt>) - Nack all messages up to this message
        # * <tt>requeue</tt> (<tt>Boolean</tt>) - Requeue the message
        # Returns <tt>nil</tt>.
        def basic_nack(delivery_tag, multiple: false, requeue: false)
          write_bytes FrameBytes.basic_nack(@id, delivery_tag, multiple, requeue)
          nil
        end

        # Reject a message
        # * <tt>delivery_tag</tt> (<tt>Integer</tt>) - The delivery tag of the message to acknowledge
        # * <tt>requeue</tt> (<tt>Boolean</tt>) - Requeue the message into the queue again
        # Returns <tt>nil</tt>.
        def basic_reject(delivery_tag, requeue: false)
          write_bytes FrameBytes.basic_reject(@id, delivery_tag, requeue)
          nil
        end

        # Recover all the unacknowledge messages
        # * <tt>requeue</tt> (<tt>Boolean</tt>) - If false the currently unack:ed messages will be deliviered to this
        #   consumer again,
        #   if true to any consumer
        # Returns <tt>nil</tt>.
        def basic_recover(requeue: false)
          write_bytes FrameBytes.basic_recover(@id, requeue:)
          expect :basic_recover_ok
          nil
        end

        # :section: Confirm

        # Put the channel in confirm mode, each published message will then be confirmed by the broker
        # * <tt>no_wait</tt> (<tt>Boolean</tt>) - If false the method will block until the broker has confirmed the request
        # Returns <tt>nil</tt>.
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
        # Returns <tt>Boolean</tt> - True if all messages were acked, false if any were nacked
        def wait_for_confirms
          @unconfirmed_lock.synchronize do
            until @unconfirmed.empty?
              # Check before waiting: if the channel was closed (and the
              # @unconfirmed_empty broadcast from #closed! fired) before we got
              # here, the wakeup is already gone and #wait would block forever.
              raise Error::Closed.new(@id, *@closed) if @closed

              @unconfirmed_empty.wait(@unconfirmed_lock)
            end
            result = !@nacked
            @nacked = false # Reset for next round of publishes
            result
          end
        end

        # Called by Connection when received ack/nack from broker
        # Internal API.
        def confirm(args)
          ack_or_nack, delivery_tag, multiple = *args
          @unconfirmed_lock.synchronize do
            # A tag we're not tracking (a duplicate, out-of-order, or broker
            # quirk) is logged and ignored, not raised: #confirm runs on the
            # read_loop thread, where an exception would tear down the connection.
            confirmed =
              if multiple
                idx = @unconfirmed.index(delivery_tag)
                @unconfirmed.shift(idx + 1) if idx
              else
                @unconfirmed.delete(delivery_tag)
              end
            if confirmed
              @nacked = true if ack_or_nack == :nack
              @unconfirmed_empty.broadcast if @unconfirmed.empty?
            else
              warn "AMQP-Client received #{ack_or_nack} for unknown delivery tag #{delivery_tag} on channel #{@id}"
            end
          end
        end

        # :section: Transaction

        # Put the channel in transaction mode, make sure that you #tx_commit or #tx_rollback after publish
        # Returns <tt>nil</tt>.
        def tx_select
          write_bytes FrameBytes.tx_select(@id)
          expect :tx_select_ok
          nil
        end

        # Commmit a transaction, requires that the channel is in transaction mode
        # Returns <tt>nil</tt>.
        def tx_commit
          write_bytes FrameBytes.tx_commit(@id)
          expect :tx_commit_ok
          nil
        end

        # Rollback a transaction, requires that the channel is in transaction mode
        # Returns <tt>nil</tt>.
        def tx_rollback
          write_bytes FrameBytes.tx_rollback(@id)
          expect :tx_rollback_ok
          nil
        end

        # Internal API.
        def reply(args)
          @replies.push(args)
        end

        # Internal API.
        def message_returned(reply_code, reply_text, exchange, routing_key)
          @next_msg = ReturnMessage.new(reply_code, reply_text, exchange, routing_key)
        end

        # Internal API.
        def message_delivered(consumer_tag, delivery_tag, redelivered, exchange, routing_key)
          @next_msg = Message.new(self, consumer_tag, delivery_tag, exchange, routing_key, redelivered)
        end

        # Internal API.
        def basic_get_empty
          @basic_gets.push :basic_get_empty
        end

        # Internal API.
        def header_delivered(body_size, properties)
          @next_msg.properties = properties
          if body_size.zero?
            next_message_finished!
          else
            @next_body = StringIO.new(String.new(capacity: body_size))
            @next_body_size = body_size
          end
        end

        # Internal API.
        def body_delivered(body_part)
          @next_body.write(body_part)
          return unless @next_body.pos == @next_body_size

          @next_msg.body = @next_body.string
          next_message_finished!
        end

        # Handle consumer cancellation from the broker
        # Internal API.
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
              t = Thread.new { @on_return.call(next_msg) }
              t.name = @connection.thread_name(role: "on_return", detail: "ch=#{@id}")
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
            rescue Error::ConnectionClosed, Error::ChannelClosed
              # The connection or channel closed while the message was being processed (e.g. an
              # ack/reject/publish from the consumer raced a shutdown). The worker can't make
              # progress and there's no bug to surface, so stop quietly instead of crashing it.
              return
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
