#!/usr/bin/env ruby
# frozen_string_literal: true

# Memory-oriented analogue of throughput_benchmark.rb.
# Measures object allocations and rough RSS deltas during publish + consume cycles
# for high-level and low-level APIs (and optionally Bunny) to enable branch vs main
# comparisons. Outputs human-readable text and optional JSON (-j path).

require_relative "../lib/amqp-client"
require "optparse"
require "json"
require "benchmark"
require "time"

begin
  require "bunny"
rescue LoadError
  # Bunny optional
end

# Benchmark measuring object allocation behavior for publishing and consuming
# AMQP messages using high-level, low-level, and optionally Bunny APIs. It
# reports raw and per-message allocation counts plus GC activity and can emit
# JSON for branch vs. main comparisons.
class MemoryThroughputBenchmark
  Cancelable = Struct.new(:cancel)

  PhaseStats = Struct.new(
    :objects_allocated,
    :objects_freed,
    :total_allocated_objects,
    :minor_gc_count,
    :major_gc_count,
    :allocated_size_bytes,
    :wall_time,
    keyword_init: true
  )

  APIResult = Struct.new(:publish, :consume, keyword_init: true)

  def initialize(message_count:, message_size_bytes:, amqp_uri:, with_bunny:, json_path: nil, warmup: true)
    @amqp_uri = amqp_uri
    @message_count = message_count
    @message_size_bytes = message_size_bytes
    @payload = "x" * message_size_bytes
    @with_bunny = with_bunny && defined?(Bunny)
    @json_path = json_path
    @warmup = warmup
  end

  def run
    print_header
    setup
    warmup_phase if @warmup

    results = {
      highlevel: test_highlevel_api,
      lowlevel: test_lowlevel_api
    }
    results[:bunny] = test_bunny if @with_bunny

    print_summary(results)
    write_json(results) if @json_path
  ensure
    cleanup
  end

  private

  def print_header
    puts "AMQP Memory Throughput Benchmark"
    puts "=" * 50
    puts "URI: #{@amqp_uri}"
    puts "Messages: #{@message_count}"
    puts "Message size: #{@message_size_bytes} bytes"
    puts "Bunny: #{@with_bunny ? 'enabled' : 'disabled'}"
    puts
  end

  def setup
    @client = AMQP::Client.new(@amqp_uri).tap(&:start)
    @connection = AMQP::Client.new(@amqp_uri).connect
    @channel = @connection.channel
    return unless @with_bunny

    @bunny = Bunny.new(@amqp_uri)
    @bunny.start
  end

  def warmup_phase
    puts "Warmup (GC + small publish cycle)"
    GC.start
    queue = @client.queue("memory_bench_warmup_#{Time.now.to_i}", durable: false, auto_delete: true)
    1000.times { queue.publish_and_forget(@payload) }
    consumed = 0
    q = ::Queue.new
    queue.subscribe(no_ack: true) do |_msg|
      consumed += 1
      q << true if consumed == 1000
    end
    q.pop
    GC.start
    puts "Warmup complete\n"
  end

  def cleanup
    @client&.stop
    @channel&.close
    @connection&.close
    @bunny&.stop
  end

  def gc_snapshot
    s = GC.stat
    {
      total_allocated_objects: s[:total_allocated_objects],
      total_freed_objects: s[:total_freed_objects],
      minor_gc_count: s[:minor_gc_count],
      major_gc_count: s[:major_gc_count]
    }
  end

  def measure_phase
    GC.start
    before = gc_snapshot
    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    wall = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
    after = gc_snapshot
    PhaseStats.new(
      objects_allocated: after[:total_allocated_objects] - before[:total_allocated_objects],
      objects_freed: after[:total_freed_objects] - before[:total_freed_objects],
      total_allocated_objects: after[:total_allocated_objects],
      minor_gc_count: after[:minor_gc_count] - before[:minor_gc_count],
      major_gc_count: after[:major_gc_count] - before[:major_gc_count],
      allocated_size_bytes: @message_count * @message_size_bytes, # rough logical bytes processed
      wall_time: wall
    )
  end

  def per_message(count)
    return 0 if count.zero?

    count.to_f / @message_count
  end

  def test_highlevel_api
    queue_name = "memory_highlevel_#{Time.now.to_i}"
    queue = @client.queue(queue_name, durable: false, auto_delete: true)
    queue.purge

    publish_stats = measure_phase do
      @message_count.times { queue.publish_and_forget(@payload) }
    end

    consume_stats = measure_phase do
      consumed = 0
      done = ::Queue.new
      queue.subscribe(no_ack: true) do |_msg|
        consumed += 1
        done << true if consumed == @message_count
      end
      done.pop
    end

    APIResult.new(publish: publish_stats, consume: consume_stats)
  end

  def test_lowlevel_api
    queue_name = "memory_lowlevel_#{Time.now.to_i}"
    @channel.queue_declare(queue_name, durable: false, auto_delete: true)
    @channel.queue_purge(queue_name)

    publish_stats = measure_phase do
      @message_count.times do
        @channel.basic_publish(@payload, exchange: "", routing_key: queue_name)
      end
    end

    consume_stats = measure_phase do
      consumed = 0
      done = ::Queue.new
      tag, = @channel.basic_consume(queue_name, no_ack: true) do |_msg|
        consumed += 1
        done << true if consumed == @message_count
      end
      done.pop
      begin
        @channel.basic_cancel(tag)
      rescue KeyError
        # Consumer may already be gone (auto-delete queue); ignore.
      end
    end

    APIResult.new(publish: publish_stats, consume: consume_stats)
  end

  def test_bunny
    ch = @bunny.create_channel
    queue_name = "memory_bunny_#{Time.now.to_i}"
    q = ch.queue(queue_name, durable: false, auto_delete: true)
    q.purge

    publish_stats = measure_phase do
      @message_count.times { q.publish(@payload) }
    end

    consume_stats = measure_phase do
      consumed = 0
      done = ::Queue.new
      q.subscribe(manual_ack: false, block: false) do |_delivery_info, _properties, _body|
        consumed += 1
        done << true if consumed == @message_count
      end
      done.pop
    end

    APIResult.new(publish: publish_stats, consume: consume_stats)
  ensure
    ch&.close
  end

  def phase_lines(label, stats)
    alloc_per_msg = per_message(stats.objects_allocated)
    freed_per_msg = per_message(stats.objects_freed)
    [
      "    #{label} wall: #{format('%.3f', stats.wall_time)}s",
      "    #{label} objs alloc: #{stats.objects_allocated} (#{format('%.2f', alloc_per_msg)} per msg)",
      "    #{label} objs freed: #{stats.objects_freed} (#{format('%.2f', freed_per_msg)} per msg)",
      "    #{label} minor GC: #{stats.minor_gc_count} major GC: #{stats.major_gc_count}"
    ]
  end

  def print_summary(results)
    puts "Summary"
    puts "=" * 50

    results.each do |api, api_res|
      puts api.to_s.capitalize
      puts "  Publish:" \
           "\n" + phase_lines("pub", api_res.publish).join("\n")
      puts "  Consume:" \
           "\n" + phase_lines("con", api_res.consume).join("\n")
      pub_per_msg = per_message(api_res.publish.objects_allocated)
      con_per_msg = per_message(api_res.consume.objects_allocated)
      puts "  Total per msg alloc (pub+con): #{format('%.2f', pub_per_msg + con_per_msg)}"
      puts
    end

    # Ratios (lowlevel vs highlevel) if both present
    return unless results[:highlevel] && results[:lowlevel]

    hl_pub = results[:highlevel].publish.objects_allocated
    ll_pub = results[:lowlevel].publish.objects_allocated
    hl_con = results[:highlevel].consume.objects_allocated
    ll_con = results[:lowlevel].consume.objects_allocated
    puts "Ratios (Low / High)"
    puts "  Publish allocations: #{ratio(ll_pub, hl_pub)}x"
    puts "  Consume allocations: #{ratio(ll_con, hl_con)}x"
    puts
  end

  def ratio(numerator, denominator)
    return 0 if denominator.zero?

    (numerator.to_f / denominator).round(2)
  end

  def write_json(results)
    data = {
      meta: {
        message_count: @message_count,
        message_size_bytes: @message_size_bytes,
        amqp_uri: @amqp_uri,
        ruby_version: RUBY_VERSION,
        time: Time.now.utc.iso8601
      },
      apis: {}
    }

    results.each do |api, api_res|
      data[:apis][api] = {
        publish: phase_json(api_res.publish),
        consume: phase_json(api_res.consume)
      }
    end

    File.write(@json_path, JSON.pretty_generate(data))
    puts "JSON written to #{@json_path}"
  end

  def phase_json(stats)
    {
      objects_allocated: stats.objects_allocated,
      objects_freed: stats.objects_freed,
      minor_gc_count: stats.minor_gc_count,
      major_gc_count: stats.major_gc_count,
      wall_time_s: stats.wall_time,
      alloc_per_message: per_message(stats.objects_allocated),
      freed_per_message: per_message(stats.objects_freed)
    }
  end
end

if __FILE__ == $PROGRAM_NAME
  options = {
    amqp_uri: "amqp://localhost",
    message_count: 50_000,
    message_size_bytes: 1024,
    with_bunny: true,
    json_path: nil,
    warmup: true
  }

  OptionParser.new do |opts|
    opts.banner = "Usage: #{$PROGRAM_NAME} [options]"
    opts.on("-u", "--uri URI", "AMQP URI (default: amqp://localhost)") { |v| options[:amqp_uri] = v }
    opts.on("-c", "--count N", Integer, "Message count (default: 50k)") { |v| options[:message_count] = v }
    opts.on("-s", "--size BYTES", Integer, "Message size bytes (default: 1024)") { |v| options[:message_size_bytes] = v }
    opts.on("--no-bunny", "Disable Bunny comparison") { options[:with_bunny] = false }
    opts.on("-j", "--json PATH", "Write JSON metrics to PATH") { |v| options[:json_path] = v }
    opts.on("--no-warmup", "Disable warmup phase") { options[:warmup] = false }
    opts.on("-h", "--help", "Show help") do
      puts opts
      exit 0
    end
  end.parse!

  bench = MemoryThroughputBenchmark.new(**options)
  bench.run
end
