#!/usr/bin/env ruby
# frozen_string_literal: true

# Consume throughput of the client alone. A fake broker, forked into its own
# process, handshakes and then streams prebuilt deliveries as fast as the socket
# takes them, so the client's CPU is the bottleneck, not the broker's.
#
# It ignores prefetch and acks, so it's for no_ack consumption only, measure
# acking against a real broker.

require_relative "../lib/amqp-client"
require "optparse"
require "socket"

# Fake AMQP 0-9-1 broker streaming deliveries to a single consumer
class Firehose
  CONSUMER_TAG = "amq.ctag-firehose"

  def initialize(body_size:, properties:)
    @body_size = body_size
    @properties = properties
  end

  # Start serving in a forked process
  # @return [Array(Integer, Integer)] port and pid
  def start
    server = TCPServer.new("127.0.0.1", 0)
    pid = fork do
      sock = server.accept
      server.close
      serve(sock)
    rescue IOError, SystemCallError
      exit! 0
    end
    port = server.addr[1]
    server.close
    [port, pid]
  end

  private

  def serve(sock)
    sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    sock.read(8) # protocol header
    frame_max = 131_072
    sock.write method_frame(0, 10, 10, [0, 9].pack("CC") + table + longstr("PLAIN") + longstr("en_US"))
    loop do
      channel_id, class_id, method_id, args = read_method(sock)
      case [class_id, method_id]
      when [10, 11] then sock.write method_frame(0, 10, 30, [2047, frame_max, 0].pack("S> L> S>"))
      when [10, 31] then frame_max = args.unpack1("L>", offset: 2) # the client's frame_max
      when [10, 40] then sock.write method_frame(0, 10, 41, shortstr(""))
      when [20, 10] then sock.write method_frame(channel_id, 20, 11, longstr(""))
      when [60, 20]
        sock.write method_frame(channel_id, 60, 21, shortstr(CONSUMER_TAG))
        stream(sock, deliveries(channel_id, frame_max))
      end
    end
  end

  # Write the same deliveries over and over, discarding whatever the client sends
  def stream(sock, bytes)
    sink = String.new(capacity: 65_536)
    offset = 0
    loop do
      readable, writable = IO.select([sock], [sock])
      raise EOFError if readable.any? && sock.read_nonblock(65_536, sink, exception: false).nil?
      next if writable.empty?

      written = sock.write_nonblock(offset.zero? ? bytes : bytes.byteslice(offset..), exception: false)
      offset = (offset + written) % bytes.bytesize if written.is_a?(Integer)
    end
  end

  # About 1 MB of deliveries
  def deliveries(channel_id, frame_max)
    body = "x" * @body_size
    header = frame(2, channel_id, [60, 0, @body_size].pack("S> S> Q>") + encoded_properties)
    out = String.new(encoding: Encoding::BINARY)
    [1, 1_048_576 / (@body_size + 100)].max.times do |i|
      out << method_frame(channel_id, 60, 60, shortstr(CONSUMER_TAG) + [i + 1, 0].pack("Q> C") + shortstr("") +
                                      shortstr("bench"))
      out << header
      0.step(@body_size - 1, frame_max - 8) { |pos| out << frame(3, channel_id, body.byteslice(pos, frame_max - 8)) }
    end
    out
  end

  def encoded_properties
    return [0].pack("S>") unless @properties

    AMQP::Client::Properties.encode(content_type: "application/json", delivery_mode: 2,
                                    message_id: "6f1c2b3a-0e7d-4f2a-9d1b-3c4e5f607182",
                                    timestamp: Time.now,
                                    headers: { "x-trace-id" => "abc123def456", "x-tenant" => "acme",
                                               "x-retry" => "0" })
  end

  def read_method(sock)
    loop do
      type, channel_id, size = sock.read(7).unpack("C S> L>")
      payload = sock.read(size)
      sock.read(1)
      return [channel_id, *payload.unpack("S> S>"), payload.byteslice(4..)] if type == 1
    end
  end

  def frame(type, channel_id, payload) = [type, channel_id, payload.bytesize].pack("C S> L>") + payload + "\xCE".b
  def method_frame(channel_id, class_id, method_id, args) = frame(1, channel_id, [class_id, method_id].pack("S> S>") + args)
  def shortstr(str) = [str.bytesize].pack("C") + str.b
  def longstr(str) = [str.bytesize].pack("L>") + str.b
  def table = [0].pack("L>")
end

options = { count: 500_000, size: 16, properties: false }
OptionParser.new do |opts|
  opts.banner = "Usage: #{$PROGRAM_NAME} [options]"
  opts.on("-c", "--count COUNT", Integer, "Number of messages (default: 500,000)") { |v| options[:count] = v }
  opts.on("-s", "--size SIZE", Integer, "Message body size in bytes (default: 16)") { |v| options[:size] = v }
  opts.on("-p", "--properties", "Deliver messages with properties and headers") { options[:properties] = true }
end.parse!

port, pid = Firehose.new(body_size: options[:size], properties: options[:properties]).start
connection = AMQP::Client.new("amqp://127.0.0.1:#{port}").connect
channel = connection.channel
done = Queue.new
count = 0
cpu_start = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)
start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
channel.basic_consume("bench", no_ack: true) do |_msg|
  done << true if (count += 1) == options[:count]
end
done.pop
elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
cpu = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) - cpu_start
Process.kill(:KILL, pid)
puts format("%<size>d bytes: %<rate>.0f msg/s, %<cpu>.2f s CPU",
            size: options[:size], rate: options[:count] / elapsed, cpu:)
$stdout.flush
exit! 0 # the read loop thread is still blocked on the killed broker's socket
