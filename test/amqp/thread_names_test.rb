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

  def test_name_from_uri_appears_in_read_loop_thread_name
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}?name=worker-1").connect
    names = Thread.list.map(&:name)

    assert(names.any? { |n| n&.start_with?("amqp.read_loop[worker-1] ") },
           "Expected a thread named 'amqp.read_loop[worker-1] ...', got: #{names.inspect}")
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

  def test_supervisor_thread_name_when_named
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}?name=worker-2").start
    client.with_connection { _1 } # wait until supervisor's read_loop is engaged
    names = Thread.list.map(&:name)

    assert_includes names, "amqp.supervisor[worker-2]"
  ensure
    client&.stop
  end
end
