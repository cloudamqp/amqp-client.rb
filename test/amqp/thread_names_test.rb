# frozen_string_literal: true

require_relative "../test_helper"

class AMQPThreadNamesTest < Minitest::Test
  def test_default_read_loop_thread_name
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").connect
    names = Thread.list.map(&:name)

    assert(names.any? { |n| n&.start_with?("amqp.read_loop ") },
           "Expected a thread named 'amqp.read_loop ...', got: #{names.inspect}")
  ensure
    connection&.close
  end

  def test_custom_prefix_flows_through
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", name: "myapp").connect
    names = Thread.list.map(&:name)

    assert(names.any? { |n| n&.start_with?("myapp.read_loop ") },
           "Expected a thread named 'myapp.read_loop ...', got: #{names.inspect}")
  ensure
    connection&.close
  end

  def test_heartbeat_thread_name
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", heartbeat: 1).connect
    names = Thread.list.map(&:name)

    assert(names.any? { |n| n&.start_with?("amqp.heartbeat ") },
           "Expected a thread named 'amqp.heartbeat ...', got: #{names.inspect}")
  ensure
    connection&.close
  end

  def test_consumer_worker_thread_names_include_channel_and_index
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").connect
    ch = connection.channel
    q = ch.queue_declare("", exclusive: true)
    ch.basic_consume(q.queue_name, worker_threads: 2, &:ack)

    matching = Thread.list.map(&:name).select { |n| n&.start_with?("amqp.consumer ") }
    suffixes = matching.map { |n| n[/ch=\d+ tag=\S+ #\d+\z/] }

    assert_equal 2, suffixes.compact.size, "Expected 2 named consumer worker threads, got: #{matching.inspect}"
  ensure
    connection&.close
  end
end
