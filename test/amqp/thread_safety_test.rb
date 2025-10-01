# frozen_string_literal: true

require_relative "../test_helper"

module AMQP
  class ThreadSafetyTest < Minitest::Test
    def setup
      @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}")
    end

    def teardown
      @client.stop if @client&.started?
    end

    def test_idempotent_start_concurrent
      threads = 5.times.map do
        Thread.new { @client.start }
      end
      threads.each(&:join)

      assert_predicate @client, :started?, "client should be started after concurrent start attempts"
    end

    def test_concurrent_publish_and_queue_declare
      @client.start
      qname = "thread_safety_q"
      # spin up consumer to ensure queue exists eventually
      decl_threads = 3.times.map do
        Thread.new do
          10.times { @client.queue(qname) }
        end
      end

      pub_threads = 5.times.map do |i|
        Thread.new do
          50.times do |j|
            @client.publish("msg-#{i}-#{j}", exchange: "amq.direct", routing_key: qname)
          end
        end
      end

      (decl_threads + pub_threads).each(&:join)
      # Sanity: ensure queue exists via a passive declare on a fresh channel
      with_low_level_connection do |conn|
        ch = conn.channel
        q_ok = ch.queue_declare(qname, passive: true)

        assert_equal qname, q_ok.queue_name
      end
    ensure
      @client.delete_queue(qname)
    end

    private

    def with_low_level_connection
      conn = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").connect
      yield conn
    ensure
      conn&.close
    end
  end
end
