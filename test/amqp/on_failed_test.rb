# frozen_string_literal: true

require_relative "../test_helper"
require "logger"
require "stringio"

class OnFailedTest < Minitest::Test
  def teardown
    @client&.stop
  end

  def test_max_retries_rejects_non_integer
    assert_raises(ArgumentError) { AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", max_retries: "3") }
  end

  def test_max_retries_rejects_negative_integer
    assert_raises(ArgumentError) { AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", max_retries: -1) }
  end

  def test_on_failed_fires_after_max_retries_and_stops_supervisor
    failed = Queue.new
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", max_retries: 2, reconnect_interval: 0,
                                                           on_failed: ->(err) { failed << err })
    original_connect = @client.method(:connect)
    attempts = 0
    replacement = lambda { |**kwargs|
      attempts += 1
      raise AMQP::Client::Error, "simulated failure" if attempts > 1

      original_connect.call(**kwargs)
    }

    @client.stub(:connect, replacement) do
      @client.start
      @client.with_connection(&:close)

      err = failed.pop(timeout: 2)

      assert_match(/simulated failure/, err.message)
      # 1 initial connect + 2 failed reconnect attempts (max_retries) before giving up
      assert_equal 3, attempts
    end

    refute_predicate @client, :started?
  end

  def test_on_failed_error_is_logged
    io = StringIO.new
    logger = Logger.new(io)
    logger.formatter = ->(_severity, _time, _progname, message) { "#{message}\n" }
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", logger:, max_retries: 0, reconnect_interval: 0,
                                                           on_failed: ->(_err) { raise "forced on_failed failure" })
    original_connect = @client.method(:connect)
    attempts = 0
    replacement = lambda { |**kwargs|
      attempts += 1
      raise AMQP::Client::Error, "simulated failure" if attempts > 1

      original_connect.call(**kwargs)
    }

    @client.stub(:connect, replacement) do
      @client.start
      @client.with_connection(&:close)

      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
      sleep 0.01 until io.string.include?("on_failed raised") ||
                       Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
    end

    assert_includes io.string, "AMQP::Client: on_failed raised: RuntimeError: forced on_failed failure"
  end

  def test_reconnect_attempts_reset_after_a_successful_reconnect
    failed = Queue.new
    # max_retries: 2 tolerates one failed attempt per cycle below without giving up; if the
    # counter didn't reset after a successful reconnect, the second cycle's failure would push
    # the (wrongly accumulated) count over the limit and trigger a spurious give-up.
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", max_retries: 2, reconnect_interval: 0,
                                                           on_failed: ->(err) { failed << err })
    original_connect = @client.method(:connect)
    fail_next = false
    replacement = lambda { |**kwargs|
      if fail_next
        fail_next = false
        raise AMQP::Client::Error, "simulated failure"
      end
      original_connect.call(**kwargs)
    }

    @client.stub(:connect, replacement) do
      @client.start

      2.times do
        fail_next = true
        @client.with_connection(&:close)
        @client.with_connection { _1 } # wait for the single-retry reconnect to settle
      end
    end

    assert_predicate @client, :started?
    assert_predicate failed, :empty?
  end
end
