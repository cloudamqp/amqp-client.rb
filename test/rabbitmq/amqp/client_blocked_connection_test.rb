# frozen_string_literal: true

require_relative "../../test_helper"

# Tests for blocked connection handling.
# Requires RabbitMQ with sudo access to rabbitmqctl.
# Run via: bundle exec rake test:rabbitmq
class AMQPClientBlockedConnectionTest < Minitest::Test
  def setup
    @connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").connect
  end

  def teardown
    @connection&.close
  end

  def test_it_can_be_blocked
    ch = @connection.channel
    system("sudo rabbitmqctl set_vm_memory_high_watermark 0.001")
    ch.basic_publish("body", exchange: "", routing_key: "q")
    sleep 0.01 # server blocks after first publish

    assert_predicate @connection, :blocked?
    system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
    sleep 0.01 # server blocks after first publish

    refute_predicate @connection, :blocked?
  ensure
    system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
  end

  def test_blocked_handler
    q = Queue.new
    @connection.on_blocked do |reason|
      q << reason
    end
    @connection.on_unblocked do
      q << nil
    end
    system("sudo rabbitmqctl set_vm_memory_high_watermark 0.001")
    ch = @connection.channel
    ch.basic_publish("", exchange: "", routing_key: "")
    reason = q.pop

    assert_equal "low on memory", reason
    system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
    unblocked = q.pop

    assert_nil unblocked
  ensure
    system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
  end
end
