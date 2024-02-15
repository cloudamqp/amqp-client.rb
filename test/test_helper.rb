# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "amqp/client"

require "minitest/autorun"

Thread.abort_on_exception = true

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

Minitest::Test.prepend TimeoutEveryTestCase
Minitest::Test.prepend SkipSudoTestCase
