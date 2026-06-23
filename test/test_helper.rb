# frozen_string_literal: true

$stdout.sync = $stderr.sync = true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "amqp/client"

require "minitest/autorun"
require "minitest/mock"
require "minitest/reporters"

Thread.abort_on_exception = true

Minitest::Reporters.use!([Minitest::Reporters::DefaultReporter.new(slow_count: 5)])

TEST_AMQP_HOST = ENV.fetch("TEST_AMQP_HOST") do
  RUBY_ENGINE == "jruby" ? "127.0.0.1" : "localhost"
end

# Ports the broker listens on, overridable so the suite can target a broker on
# non-default ports (e.g. the standalone one bin/test-tls starts). The TLS
# tests use TEST_AMQPS_PORT; everything else uses the plaintext TEST_AMQP_PORT.
TEST_AMQP_PORT = ENV.fetch("TEST_AMQP_PORT", "5672")
TEST_AMQPS_PORT = ENV.fetch("TEST_AMQPS_PORT", "5671")

# Almost every test connects to a broker on TEST_AMQP_HOST. Verify one is
# reachable up front so the suite aborts immediately with a clear message
# instead of every test hanging until its 60s timeout.
begin
  AMQP::Client.new("amqp://#{TEST_AMQP_HOST}:#{TEST_AMQP_PORT}", connect_timeout: 3).connect.close
rescue StandardError => e
  abort "No AMQP broker reachable at amqp://#{TEST_AMQP_HOST}:#{TEST_AMQP_PORT} (#{e.class}: #{e.message}). " \
        "Start a broker or set TEST_AMQP_HOST/TEST_AMQP_PORT."
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

# For tests that exercise RabbitMQ-only broker behaviour (e.g. Connection.Blocked,
# which LavinMQ doesn't implement). rabbitmqctl on PATH is the signal that we're
# running against RabbitMQ; its apt package installs it under /usr/sbin.
module SkipRabbitMQTestCase
  def skip_unless_rabbitmqctl
    return if rabbitmqctl?

    skip "requires RabbitMQ; rabbitmqctl not found (LavinMQ doesn't send Connection.Blocked)"
  end

  def rabbitmqctl?
    search = ENV["PATH"].to_s.split(File::PATH_SEPARATOR) + %w[/usr/sbin /usr/local/sbin /sbin]
    search.any? { |dir| File.executable?(File.join(dir, "rabbitmqctl")) }
  end
end

require "socket"
require "tmpdir"
require "fileutils"

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

module ReadLoopHelpers
  # Shutting down a connection's socket makes its blocked read loop return EOF on
  # every engine (a plain close doesn't reliably wake the reader on truffleruby).
  # The read loop then closes that same socket from its own ensure the instant
  # the read returns, which on JRuby races with our shutdown and surfaces as an
  # already-closed socket. The error is engine- and socket-dependent: IOError on
  # MRI/JRuby, SystemCallError (EBADF) on truffleruby, an OpenSSL::OpenSSLError
  # for a TLS socket, sometimes a wrapped java.lang.NullPointerException on JRuby.
  # We mirror the connection's own READ_EXCEPTIONS. Losing the race is harmless —
  # it only happens once the read loop has already exited, which is exactly the
  # wakeup we're triggering.
  SOCKET_TEARDOWN_ERRORS = [IOError, OpenSSL::OpenSSLError, SystemCallError,
                            (java.lang.NullPointerException if RUBY_ENGINE == "jruby")].compact.freeze

  def shutdown_read_loop(connection)
    connection.instance_variable_get(:@socket).shutdown(Socket::SHUT_RDWR)
  rescue *SOCKET_TEARDOWN_ERRORS
    nil
  end
end

# Boots a throwaway LavinMQ instance for tests that need broker behaviour we
# can't toggle on the shared server. These tests are opt-in because starting a
# second LavinMQ currently requires clearing the hardcoded control socket path.
module LavinMQServer
  LAVINMQ_CONTROL_SOCKET = "/tmp/lavinmqctl.sock"
  LAVINMQ_FLOW_CONTROL_OPT_IN = "RUN_LAVINMQ_FLOW_CONTROL_TESTS"

  # Yields the AMQP port of a private LavinMQ that believes it is out of disk
  # space (free_disk_min above any real free space), so it applies flow control
  # and rejects publishes/declarations with PRECONDITION_FAILED.
  def with_low_disk_lavinmq(&)
    skip_unless_lavinmq_flow_control_tests

    exe = lavinmq_executable
    skip "lavinmq executable not found" unless exe

    boot_low_disk_lavinmq(exe, &)
  end

  private

  # Spawn a private LavinMQ with free_disk_min above real free space, yield its
  # AMQP port, then tear it down.
  def boot_low_disk_lavinmq(exe)
    dir = Dir.mktmpdir("lavinmq-lowdisk")
    amqp_port, http_port, mqtt_port, metrics_port = free_tcp_ports(4)
    File.write(File.join(dir, "lavinmq.ini"),
               "[main]\ndata_dir = #{dir}/data\nfree_disk_min = 1000000000000000\n")
    clear_lavinmq_control_socket(required: true)
    pid = spawn(exe, "--config", File.join(dir, "lavinmq.ini"),
                "--amqp-port", amqp_port.to_s, "--http-port", http_port.to_s,
                "--mqtt-port", mqtt_port.to_s, "--metrics-http-port", metrics_port.to_s,
                "--bind", "127.0.0.1", out: File::NULL, err: File::NULL)
    wait_for_tcp("127.0.0.1", amqp_port)
    yield amqp_port
  ensure
    stop_process(pid)
    clear_lavinmq_control_socket(required: false)
    FileUtils.remove_entry(dir) if dir
  end

  def skip_unless_lavinmq_flow_control_tests
    return if %w[1 true].include?(ENV[LAVINMQ_FLOW_CONTROL_OPT_IN])

    skip "set #{LAVINMQ_FLOW_CONTROL_OPT_IN}=1 to run LavinMQ flow-control tests"
  end

  # LavinMQ < 2.9.0 hardcodes its control socket at /tmp/lavinmqctl.sock and
  # aborts at startup if it can't recreate it. This can be removed when LavinMQ
  # 2.9.0 is released with https://github.com/cloudamqp/lavinmq/pull/2029.
  def clear_lavinmq_control_socket(required:)
    return if system("rm", "-f", LAVINMQ_CONTROL_SOCKET, out: File::NULL, err: File::NULL)
    return if system("sudo", "-n", "rm", "-f", LAVINMQ_CONTROL_SOCKET, out: File::NULL, err: File::NULL)

    skip "requires removing #{LAVINMQ_CONTROL_SOCKET}" if required
  end

  def lavinmq_executable
    ENV["PATH"].to_s.split(File::PATH_SEPARATOR).each do |dir|
      exe = File.join(dir, "lavinmq")
      return exe if File.executable?(exe)
    end
    nil
  end

  # Probe distinct ports for the child process to bind. These are available when
  # probed, but not reserved after this method returns.
  def free_tcp_ports(count)
    servers = Array.new(count) { TCPServer.new("127.0.0.1", 0) }
    servers.map { |s| s.addr[1] }
  ensure
    servers&.each(&:close)
  end

  def wait_for_tcp(host, port, timeout: 10)
    deadline = monotonic_now + timeout
    loop do
      Socket.tcp(host, port, connect_timeout: 1).close
      return
    rescue SystemCallError
      raise "lavinmq did not start on #{host}:#{port}" if monotonic_now > deadline

      sleep 0.1
    end
  end

  def monotonic_now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def stop_process(pid)
    return unless pid

    Process.kill("TERM", pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end
end

# Socket stand-in that hands out preset byte chunks across successive reads,
# so tests can drive the handshake parser deterministically (no real network,
# no timing). Used to reproduce frame headers that arrive split across reads.
class ChunkedSocket
  def initialize(*chunks)
    @chunks = chunks
  end

  def setsockopt(*); end
  def write(*); end
  def close; end

  def readpartial(_maxlen, outbuf = +"")
    raise EOFError, "end of file reached" if @chunks.empty?

    outbuf.replace(@chunks.shift)
  end
end

$VERBOSE = nil unless ENV["DEBUG"] == "true"

Minitest::Test.prepend TimeoutEveryTestCase
Minitest::Test.prepend SkipSudoTestCase
Minitest::Test.prepend SkipRabbitMQTestCase
Minitest::Test.prepend FakeServer
Minitest::Test.prepend ThreadHelpers
Minitest::Test.prepend ReadLoopHelpers
Minitest::Test.prepend LavinMQServer
