# frozen_string_literal: true

require_relative "../test_helper"
require "logger"

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

  def test_consumer_refused_on_reconnect_waits_until_queue_is_released
    restored = Queue.new
    @client.stop
    on_connect = ->(_) { restored << true }
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", reconnect_interval: 0.01, on_connect:).start
    restored.pop(timeout: 5)
    consumer = @client.queue(QUEUE).subscribe(exclusive: true) { |msg| @msgs << msg }

    # Hold the reconnect until another client has taken the queue
    connect = @client.method(:connect)
    taken = Queue.new
    @client.stub(:connect, ->(**opts) { taken.pop(timeout: 5) && connect.call(**opts) }) do
      @client.with_connection(&:close)
      holding = @queue.subscribe(exclusive: true) { |_msg| nil }
      taken << true
      restored.pop(timeout: 5)

      refute_predicate consumer, :active?

      holding.cancel
      @queue.publish("after release")

      assert_equal "after release", @msgs.pop(timeout: 5)&.body
    end
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
end
