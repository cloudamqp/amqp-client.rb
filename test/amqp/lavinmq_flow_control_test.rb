# frozen_string_literal: true

require_relative "../test_helper"

class AMQPLavinMQFlowControlTest < Minitest::Test
  def test_low_disk_blocks_publishing_with_precondition_failed
    with_low_disk_lavinmq do |port|
      conn = AMQP::Client.new("amqp://127.0.0.1:#{port}").connect
      begin
        ch = conn.channel
        assert_raises(AMQP::Client::Error::PreconditionFailed) do
          ch.basic_publish_confirm("body", exchange: "", routing_key: "low.disk.test")
        end
      ensure
        conn&.close
      end
    end
  end

  def test_low_disk_lavinmq_startup_error_includes_broker_output
    Dir.mktmpdir("fake-lavinmq") do |dir|
      exe = File.join(dir, "lavinmq")
      File.write(exe, <<~RUBY)
        #!/usr/bin/env ruby
        puts "fake stdout"
        warn "fake stderr"
        exit 42
      RUBY
      FileUtils.chmod(0o755, exe)

      error = assert_raises(RuntimeError) do
        send(:start_low_disk_lavinmq, exe, dir)
      end

      expected = Regexp.new(
        [
          "lavinmq did not start after 3 attempts",
          "at=error attempt=1",
          "reason=exited exit_status=42",
          "fake stdout",
          "fake stderr"
        ].join(".*"),
        Regexp::MULTILINE
      )

      assert_match expected, error.message
    end
  end
end
