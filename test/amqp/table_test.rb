# frozen_string_literal: true

require_relative "../test_helper"

class AMQPTableTest < Minitest::Test
  # Field types the client never encodes itself, but other clients and brokers send
  def test_it_decodes_big_endian_fields_from_other_clients
    bytes = field("short", "s", [-2].pack("s>")) +
            field("ushort", "u", [65_534].pack("S>")) +
            field("bytes", "x", [3, "abc"].pack("L>a*"))

    assert_equal({ "short" => -2, "ushort" => 65_534, "bytes" => "abc" }, AMQP::Client::Table.decode(bytes))
  end

  def test_it_roundtrips_integers_outside_the_32_bit_range
    [
      2**31,           # smallest value that overflows a signed 32-bit int
      -(2**31) - 1,    # largest-magnitude negative value that overflows it
      -(2**53 - 1),    # a large negative 64-bit value, e.g. a JS-originated number
      2**32,
    ].each do |value|
      bytes = AMQP::Client::Table.encode({ "k" => value })
      assert_equal({ "k" => value }, AMQP::Client::Table.decode(bytes), "failed to roundtrip #{value}")
    end
  end

  private

  def field(key, type, value)
    [key.bytesize, key, type].pack("Ca*a") + value
  end
end
