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

  private

  def field(key, type, value)
    [key.bytesize, key, type].pack("Ca*a") + value
  end
end
