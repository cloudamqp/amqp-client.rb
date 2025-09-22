#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "throughput_benchmark"

def calculate_stats(values)
  sorted = values.sort
  n = values.length

  {
    mean: values.sum.to_f / n,
    median: n.odd? ? sorted[n / 2] : (sorted[(n / 2) - 1] + sorted[n / 2]) / 2.0,
    min: sorted.first,
    max: sorted.last,
    p95: sorted[(n * 0.95).ceil - 1],
    std_dev: Math.sqrt(values.sum { |v| (v - (values.sum.to_f / n))**2 } / n)
  }
end

def format_stats(label, stats)
  puts "#{label}:"
  puts "  Mean:   #{stats[:mean].round(1)} msg/s"
  puts "  Median: #{stats[:median].round(1)} msg/s"
  puts "  Min:    #{stats[:min].round(1)} msg/s"
  puts "  Max:    #{stats[:max].round(1)} msg/s"
  puts "  95th %: #{stats[:p95].round(1)} msg/s"
  puts "  Std Dev: #{stats[:std_dev].round(1)} msg/s"
  puts
end

puts "Running benchmark multiple times for statistical analysis..."
puts "=" * 60

iterations = 5
high_level_pub = []
low_level_pub = []
high_level_con = []
low_level_con = []

iterations.times do |i|
  print "Run #{i + 1}/#{iterations}... "

  benchmark = ThroughputBenchmark.new(
    message_count: 50_000,
    message_size_bytes: 256,
    amqp_uri: "amqp://localhost",
    with_bunny: false
  )

  results = benchmark.run

  high_level_pub << results[:highlevel][:publish][:rate]
  low_level_pub << results[:lowlevel][:publish][:rate]
  high_level_con << results[:highlevel][:consume][:rate]
  low_level_con << results[:lowlevel][:consume][:rate]

  puts "done"
end

puts "\nStatistical Results (#{iterations} runs):"
puts "=" * 60

format_stats("High-Level Publishing", calculate_stats(high_level_pub))
format_stats("Low-Level Publishing", calculate_stats(low_level_pub))
format_stats("High-Level Consuming", calculate_stats(high_level_con))
format_stats("Low-Level Consuming", calculate_stats(low_level_con))

hl_stats = calculate_stats(high_level_pub)
ll_stats = calculate_stats(low_level_pub)

puts "Performance Gap Analysis:"
puts "=" * 30
puts "Low-Level vs High-Level Ratio:"
puts "  Mean:   #{(ll_stats[:mean] / hl_stats[:mean]).round(2)}x"
puts "  Median: #{(ll_stats[:median] / hl_stats[:median]).round(2)}x"
puts "  Min:    #{(ll_stats[:min] / hl_stats[:min]).round(2)}x"
puts "  Max:    #{(ll_stats[:max] / hl_stats[:max]).round(2)}x"
