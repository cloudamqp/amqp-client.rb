# frozen_string_literal: true

$stdout.sync = $stderr.sync = true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "amqp/client"

require "minitest/autorun"
require "minitest/reporters"

Thread.abort_on_exception = true

Minitest::Reporters.use!([Minitest::Reporters::DefaultReporter.new(slow_count: 5)])

TEST_AMQP_HOST = ENV.fetch("TEST_AMQP_HOST") do
  RUBY_ENGINE == "jruby" ? "127.0.0.1" : "localhost"
end

require "timeout"
module TimeoutEveryTestCase
  # our own subclass so we never confused different timeouts
  class TestTimeout < Timeout::Error
    def self.limit
      60
    end
  end

  def capture_exceptions(&block)
    super do
      ::Timeout.timeout(TestTimeout.limit, TestTimeout, "timed out after #{TestTimeout.limit} seconds", &block)
    end
  end
end

module SkipSudoTestCase
  def skip_if_no_sudo
    skip "requires sudo" unless %w[1 true].include?(ENV["RUN_SUDO_TESTS"])
  end
end

require "socket"
module FakeServer
  def with_fake_server(host: "127.0.0.1")
    server = TCPServer.new(host, 0)

    Thread.new do
      loop do
        client = server.accept
        client.puts "foobar"
        client.close
        break
      rescue IOError
        break
      end
    end

    yield server.connect_address.ip_port

    server.close
  end
end

module ThreadHelpers
  # Run the block in a thread that records its outcome — the return value, or
  # any rescued StandardError — into a Queue, then wait until that thread is
  # parked (status "sleep") or finished. The wait is bounded by `timeout` so a
  # thread that finishes early or never parks can't spin the suite forever.
  # Returns [thread, queue]; the caller triggers whatever unblocks it, pops the
  # queue, and must clean the thread up (e.g. `thread&.kill&.join` in an ensure).
  def blocked_thread(timeout: 5)
    mailbox = Queue.new
    thread = Thread.new do
      mailbox.push(yield)
    rescue StandardError => e
      mailbox.push(e)
    end
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    while thread.alive? && thread.status != "sleep"
      break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.01
    end
    [thread, mailbox]
  end
end

$VERBOSE = nil unless ENV["DEBUG"] == "true"

Minitest::Test.prepend TimeoutEveryTestCase
Minitest::Test.prepend SkipSudoTestCase
Minitest::Test.prepend FakeServer
Minitest::Test.prepend ThreadHelpers
