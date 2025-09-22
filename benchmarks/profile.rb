#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "stackprof"
require "fileutils"
require_relative "../lib/amqp-client"

# Focused profiling script that only tests our AMQP client (no Bunny)
class AMQPClientProfiler
  def initialize
    @amqp_uri = ENV.fetch("RABBITMQ_URL", "amqp://localhost")
    @message_count = 10_000 # Smaller count for focused profiling
    @message_size = 256
    @test_data = "x" * @message_size
  end

  def run_profiled_benchmark
    puts "AMQP Client Focused Profiling"
    puts "=" * 40
    puts "URI: #{@amqp_uri}"
    puts "Messages: #{@message_count}"
    puts "Message size: #{@message_size} bytes"
    puts ""

    FileUtils.mkdir_p(File.join(__dir__, "..", "tmp"))

    # Profile high-level API (where performance gap exists)
    puts "Profiling High-Level API Publishing..."

    StackProf.run(mode: :cpu, interval: 100, out: "tmp/stackprof-high-level.dump") do
      profile_high_level_publishing
    end

    puts "High-level profile saved to tmp/stackprof-high-level.dump"
    puts ""

    # Profile low-level API for comparison
    puts "Profiling Low-Level API Publishing..."

    StackProf.run(raw: true, mode: :cpu, interval: 100, out: "tmp/stackprof-low-level.dump") do
      profile_low_level_publishing
    end

    puts "Low-level profile saved to tmp/stackprof-low-level.dump"
    puts ""

    puts "Focused profiling complete!"
    puts ""
    puts "Analyze with:"
    puts "  bundle exec stackprof tmp/stackprof-high-level.dump --text --limit=30"
    puts "  bundle exec stackprof tmp/stackprof-low-level.dump --text --limit=30"
    puts ""
    puts "Compare the two profiles to see where high-level API loses performance:"
    puts "  bundle exec stackprof tmp/stackprof-high-level.dump --method='AMQP::Client::Properties.encode'"
    puts "  bundle exec stackprof tmp/stackprof-high-level.dump --method='publish'"
    puts "  bundle exec stackprof tmp/stackprof-high-level.dump --method='AMQP::Client::Connection'"
    puts ""
    puts "For call graphs:"
    puts "  bundle exec stackprof tmp/stackprof-high-level.dump --flamegraph > tmp/high-level-flamegraph.txt"
    puts "  bundle exec stackprof tmp/stackprof-low-level.dump --flamegraph > tmp/low-level-flamegraph.txt"
  end

  private

  def profile_high_level_publishing
    client = AMQP::Client.new(@amqp_uri)
    client.start

    begin
      queue_name = "profile_highlevel_#{Time.now.to_i}"
      queue = client.queue(queue_name, durable: false, auto_delete: true)
      queue.purge

      @message_count.times do
        queue.publish_and_forget(@test_data)
      end
    ensure
      client.stop
    end
  end

  def profile_low_level_publishing
    connection = AMQP::Client.new(@amqp_uri).connect

    begin
      ch = connection.channel
      queue_name = "profile_lowlevel_#{Time.now.to_i}"
      ch.queue_declare(queue_name, durable: false, auto_delete: true)
      ch.queue_purge(queue_name)

      @message_count.times do
        ch.basic_publish(@test_data, exchange: "", routing_key: queue_name, delivery_mode: 2)
      end
    ensure
      ch&.close
      connection&.close
    end
  end
end

# Run the focused profiler
profiler = AMQPClientProfiler.new
profiler.run_profiled_benchmark
