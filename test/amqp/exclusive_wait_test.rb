# frozen_string_literal: true

require_relative "../test_helper"
require "logger"
require "stringio"

# Consumers waiting for a queue in exclusive use, e.g. during a rolling deploy where the
# previous process still holds the queue. The waiting client retries every
# reconnect_interval, kept short so the tests finish as soon as the queue is released.
class ExclusiveWaitTest < Minitest::Test
  QUEUE = "test.exclusive.wait"

  def setup
    @holder = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", reconnect_interval: 0.01).start
    @queue = @holder.queue(QUEUE)
    @msgs = Queue.new
  end

  def teardown
    @client.stop
    @queue.delete
    @holder.stop
  end

  def test_subscribe_waits_until_queue_is_released
    holding = @queue.subscribe(exclusive: true) { |_msg| nil }
    waiting = @client.queue(QUEUE).subscribe(exclusive: :wait) { |msg| @msgs << msg }

    refute_predicate waiting, :active?

    holding.cancel
    @queue.publish("after release")

    assert_equal "after release", @msgs.pop(timeout: 5)&.body
    assert_predicate waiting, :active?
  end

  def test_waiting_consumer_can_be_cancelled
    @queue.subscribe(exclusive: true) { |_msg| nil }
    waiting = @client.queue(QUEUE).subscribe(exclusive: :wait) { |msg| @msgs << msg }

    waiting.cancel

    assert_predicate waiting, :closed?
    refute_includes @client.instance_variable_get(:@consumers).values, waiting
  end

  def test_waiting_consumer_refused_on_reconnect_waits_until_queue_is_released
    restored = restart_client
    consumer = @client.queue(QUEUE).subscribe(exclusive: :wait) { |msg| @msgs << msg }

    holding = reconnect_after_queue_is_taken(restored)

    refute_predicate consumer, :active?

    holding.cancel
    @queue.publish("after release")

    assert_equal "after release", @msgs.pop(timeout: 5)&.body
  end

  def test_exclusive_consumer_refused_on_reconnect_is_cancelled
    log = StringIO.new
    restored = restart_client(logger: Logger.new(log))
    cancelled = Queue.new
    consumer = @client.queue(QUEUE).subscribe(exclusive: true, on_cancel: ->(tag) { cancelled << tag }) { |_msg| nil }
    tag = consumer.tag

    reconnect_after_queue_is_taken(restored)

    assert_equal tag, cancelled.pop(timeout: 5)
    assert_predicate consumer, :closed?
    assert_match(/ERROR .*#{QUEUE}/, log.string)
  end

  def test_stop_ends_the_retry_thread_while_disconnected
    @queue.subscribe(exclusive: true) { |_msg| nil }
    @client.stop
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", reconnect_interval: 0.01, logger: Logger.new(nil)).start
    @client.queue(QUEUE).subscribe(exclusive: :wait) { |msg| @msgs << msg }
    retry_thread = Thread.list.find { |t| t.name == "amqp.consumer_retry" }

    # Keep the client disconnected, so the retry thread blocks waiting for a connection
    @client.stub(:connect, ->(**) { raise AMQP::Client::Error, "broker down" }) do
      @client.with_connection(&:close)
      Timeout.timeout(5) { sleep 0.001 until retry_thread.backtrace&.any? { |l| l.include?("Queue#pop") } }
      @client.stop
    end

    assert retry_thread.join(5), "retry thread still running after stop"
  end

  private

  # Replaces @client with one signalling each (re)connect, once it has resubscribed its consumers
  def restart_client(**)
    restored = Queue.new
    @client.stop
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", reconnect_interval: 0.01,
                                                           on_connect: ->(_) { restored << true }, **).start
    restored.pop(timeout: 5)
    restored
  end

  # Drops @client's connection and holds the reconnect until another client has taken the queue
  # as an exclusive consumer. Returns that consumer once @client has reconnected.
  def reconnect_after_queue_is_taken(restored)
    connect = @client.method(:connect)
    taken = Queue.new
    @client.stub(:connect, ->(**opts) { taken.pop(timeout: 5) && connect.call(**opts) }) do
      @client.with_connection(&:close)
      holding = @queue.subscribe(exclusive: true) { |_msg| nil }
      taken << true
      restored.pop(timeout: 5)
      holding
    end
  end
end
