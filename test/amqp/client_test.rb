# frozen_string_literal: true

require_relative "../test_helper"

# Specs that does not use connection setup/teardown (see client_lifecycle_test.rb)
class AMQPClientLifecycleTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::AMQP::Client::VERSION
  end

  def test_it_raises_on_connecting_to_unrelated_service
    with_fake_server do |port|
      client = AMQP::Client.new("amqp://guest1:guest2@#{TEST_AMQP_HOST}:#{port}")
      assert_raises(AMQP::Client::Error) do
        client.connect
      end
    end
  end

  def test_it_raises_on_bad_credentials
    client = AMQP::Client.new("amqp://guest1:guest2@#{TEST_AMQP_HOST}")
    assert_raises(AMQP::Client::Error) do
      client.connect
    end
  end

  def test_it_can_stop
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}")
    client.stop

    assert client
  end

  def test_set_connection_name
    skip "slow, polls HTTP mgmt API"
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", connection_name: "foobar")
    client.connect

    req = Net::HTTP::Get.new("/api/@connections?columns=client_properties")
    req.basic_auth "guest", "guest"
    http = Net::HTTP.new(TEST_AMQP_HOST, 15_672)
    connection_names = []
    100.times do
      sleep 0.1
      res = http.request(req)

      assert_instance_of Net::HTTPOK, res.class
      connection_names = JSON.parse(res.body).map! { |conn| conn.dig("client_properties", "@connection_name") }
      break if connection_names.include? "foobar"
    end

    assert_includes connection_names, "foobar"
  end

  def test_it_can_set_channel_max
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", channel_max: 1).connect

    assert connection.channel
    assert_raises(AMQP::Client::Error) do
      connection.channel
    end
  ensure
    connection&.close
  end
end
