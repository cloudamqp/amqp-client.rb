# frozen_string_literal: true

$stdout.sync = $stderr.sync = true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "amqp/client"

require "minitest/autorun"

Thread.abort_on_exception = true

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

  def run
    capture_exceptions do
      ::Timeout.timeout(TestTimeout.limit,
                        TestTimeout,
                        "timed out after #{TestTimeout.limit} seconds") do
        before_setup
        setup
        after_setup
        send(name)
      end
    end
    Minitest::Result.from(self)
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

$VERBOSE = nil unless ENV["DEBUG"] == "true"

Minitest::Test.prepend TimeoutEveryTestCase
Minitest::Test.prepend SkipSudoTestCase
Minitest::Test.prepend FakeServer
