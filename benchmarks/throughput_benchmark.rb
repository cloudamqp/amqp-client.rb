#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "../lib/amqp-client"
require "benchmark"
require "bunny"

# Throughput benchmark for AMQP client
class ThroughputBenchmark
  def initialize(message_count:, message_size_bytes:, amqp_uri: "amqp://localhost")
    @amqp_uri = amqp_uri
    @message_count = message_count
    @message_size_bytes = message_size_bytes
    @test_data = "x" * @message_size_bytes
  end

  def run
    puts "AMQP Throughput Benchmark"
    puts "=" * 40
    puts "URI: #{@amqp_uri}"
    puts "Messages: #{@message_count}"
    puts "Message size: #{@message_size_bytes} bytes"
    puts

    setup

    results = {}

    # Test high-level API
    puts "High-Level API Tests"
    puts "-" * 20
    results[:highlevel] = test_highlevel_api
    puts

    # Test low-level API
    puts "Low-Level API Tests"
    puts "-" * 20
    results[:lowlevel] = test_lowlevel_api
    puts

    # Test Bunny
    puts "Bunny API Tests"
    puts "-" * 20
    results[:bunny] = test_bunny

    print_summary(results)

    cleanup
    results
  end

  private

  def setup
    # High-level API setup
    @client = AMQP::Client.new(@amqp_uri)
    @client.start

    # Low-level API setup
    @connection = AMQP::Client.new(@amqp_uri).connect
    @channel = @connection.channel

    # Bunny
    @bunny = Bunny.new
    @bunny.start
  end

  def cleanup
    @client&.stop
    @channel&.close
    @connection&.close
    @bunny&.stop
  end

  # Generic method to benchmark publishing operations
  def benchmark_publishing(&block)
    puts "  Publishing #{@message_count} messages..."

    publish_time = Benchmark.realtime do
      @message_count.times(&block)
    end

    publish_rate = @message_count / publish_time
    publish_throughput = (@message_count * @message_size_bytes) / publish_time / 1024 / 1024 # MB/s

    puts "    Duration: #{publish_time.round(3)}s"
    puts "    Rate: #{publish_rate.round(1)} msg/s"
    puts "    Throughput: #{publish_throughput.round(1)} MB/s"

    {
      duration: publish_time,
      rate: publish_rate,
      throughput_mbps: publish_throughput
    }
  end

  # Generic method to benchmark consuming operations
  def benchmark_consuming
    puts "  Consuming #{@message_count} messages..."

    consumed_count = 0
    done_q = ::Queue.new

    # Callback that tracks consumed messages
    consume_callback = proc do |_msg|
      consumed_count += 1
      done_q.push(nil) if consumed_count == @message_count
    end

    consumer = nil
    consume_time = Benchmark.realtime do
      consumer = yield(consume_callback)
      done_q.pop # Wait until all messages are consumed
    end

    # Cancel the consumer if it supports cancel
    consumer&.cancel if consumer.respond_to?(:cancel)

    consume_rate = consumed_count.positive? ? consumed_count / consume_time : 0
    consume_throughput = (consumed_count * @message_size_bytes) / consume_time / 1024 / 1024 # MB/s

    puts "    Duration: #{consume_time.round(3)}s"
    puts "    Rate: #{consume_rate.round(1)} msg/s"
    puts "    Throughput: #{consume_throughput.round(1)} MB/s"
    puts "    Messages consumed: #{consumed_count}/#{@message_count}"

    {
      duration: consume_time,
      rate: consume_rate,
      throughput_mbps: consume_throughput,
      messages_consumed: consumed_count
    }
  end

  def test_highlevel_api
    queue_name = "benchmark_highlevel_#{Time.now.to_i}"
    queue = @client.queue(queue_name, durable: false, auto_delete: true)
    queue.purge

    results = {}

    # Test publishing
    results[:publish] = benchmark_publishing do
      queue.publish_and_forget(@test_data)
    end

    # Test consuming
    results[:consume] = benchmark_consuming do |consume_callback|
      queue.subscribe(no_ack: true, &consume_callback)
    end

    results
  end

  def test_lowlevel_api
    queue_name = "benchmark_lowlevel_#{Time.now.to_i}"
    @channel.queue_declare(queue_name, durable: false, auto_delete: true)
    @channel.queue_purge(queue_name)
    results = {}

    # Test publishing
    results[:publish] = benchmark_publishing do
      @channel.basic_publish(@test_data, "", queue_name)
    end

    # Test consuming
    results[:consume] = benchmark_consuming do |consume_callback|
      tag, = @channel.basic_consume(queue_name, no_ack: true, &consume_callback)
      # Return an object that can be canceled
      OpenStruct.new(cancel: -> { @channel.basic_cancel(tag) })
    end

    results
  end

  def test_bunny
    channel = @bunny.channel
    queue_name = "benchmark_bunny_#{Time.now.to_i}"
    queue = channel.queue(queue_name, durable: false, auto_delete: true)
    queue.purge
    results = {}

    # Test publishing
    results[:publish] = benchmark_publishing do
      queue.publish(@test_data)
    end

    # Test consuming
    results[:consume] = benchmark_consuming do |consume_callback|
      queue.subscribe(manual_ack: false, &consume_callback)
    end

    results
  ensure
    channel&.close
  end

  def print_summary(results)
    puts ""
    puts "Summary"
    puts "=" * 40

    hl_pub = results[:highlevel][:publish][:rate]
    hl_con = results[:highlevel][:consume][:rate]
    ll_pub = results[:lowlevel][:publish][:rate]
    ll_con = results[:lowlevel][:consume][:rate]
    b_pub = results[:bunny][:publish][:rate]
    b_con = results[:bunny][:consume][:rate]

    puts "Publishing Rate:"
    puts "  High-level: #{hl_pub.round(1)} msg/s"
    puts "  Low-level:  #{ll_pub.round(1)} msg/s"
    puts "  Bunny:      #{b_pub.round(1)}  msg/s"
    puts "  L/H Ratio:  #{(ll_pub / hl_pub).round(1)}x"
    puts "  L/B Ratio:  #{(ll_pub / b_pub).round(1)}x"
    puts "  H/B Ratio:  #{(hl_pub / b_pub).round(1)}x"
    puts

    puts "Consuming Rate:"
    puts "  High-level: #{hl_con.round(1)} msg/s"
    puts "  Low-level:  #{ll_con.round(1)} msg/s"
    puts "  Bunny:      #{b_con.round(1)}  msg/s"
    puts "  L/H Ratio:  #{(ll_con / hl_con).round(1)}x"
    puts "  L/B Ratio:  #{(ll_con / b_con).round(1)}x"
    puts "  H/B Ratio:  #{(hl_con / b_con).round(1)}x"
  end
end

# Command line interface
if __FILE__ == $PROGRAM_NAME
  require "optparse"

  options = {
    amqp_uri: "amqp://localhost",
    message_count: 100_000,
    message_size_bytes: 1024
  }

  OptionParser.new do |opts|
    opts.banner = "Usage: #{$PROGRAM_NAME} [options]"

    opts.on("-u", "--uri URI", "AMQP URI (default: amqp://localhost)") do |v|
      options[:amqp_uri] = v
    end

    opts.on("-c", "--count COUNT", Integer, "Number of messages (default: 100,000)") do |v|
      options[:message_count] = v
    end

    opts.on("-s", "--size SIZE", Integer, "Message size in bytes (default: 1024)") do |v|
      options[:message_size_bytes] = v
    end

    opts.on("-h", "--help", "Show this help") do
      puts opts
      exit
    end
  end.parse!

  benchmark = ThroughputBenchmark.new(**options)
  benchmark.run
end
