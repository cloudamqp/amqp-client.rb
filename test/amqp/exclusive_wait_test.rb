# frozen_string_literal: true

require_relative "../test_helper"

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
end
