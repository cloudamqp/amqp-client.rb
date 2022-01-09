# AMQP::Client

A modern AMQP 0-9-1 Ruby client. Very fast (just as fast as the Java client, and >4x than other Ruby clients), fully thread-safe, blocking operations and straight-forward error handling.

It's small, only ~1800 lines of code, and without any dependencies. Other Ruby clients are about 4 times bigger. But without trading functionallity.

It's safe by default, messages are published as persistent, and is waiting for confirmation from the broker. That can of course be disabled if performance is a priority.

## Support

The library is fully supported by [CloudAMQP](https://www.cloudamqp.com), the largest RabbitMQ hosting provider in the world. Open [an issue](https://github.com/cloudamqp/amqp-client.rb/issues) or [email our support](mailto:support@cloudamqp.com) if you have problems or questions.

## Documentation

[API reference](https://cloudamqp.github.io/amqp-client.rb/)

## Usage

The client has two APIs.

### Low level API

This API matches the AMQP protocol very well, it can do everything the protocol allows, but requires some knowledge about the protocol, and doesn't handle reconnects.

```ruby
require "amqp-client"

# Opens and establishes a connection
conn = AMQP::Client.new("amqp://guest:guest@localhost").connect

# Open a channel
ch = conn.channel

# Create a temporary queue
q = ch.queue_declare

# Publish a message to said queue
ch.basic_publish_confirm "Hello World!", "", q.queue_name, persistent: true

# Poll the queue for a message
msg = ch.basic_get(q.queue_name)

# Print the message's body to STDOUT
puts msg.body
```

### High level API

The library provides a high-level API that is a bit easier to get started with, and also handles reconnection automatically.

```ruby
# Start the client, it will connect and once connected it will reconnect if that connection is lost
# Operation pending when the connection is lost will raise an exception (not timeout)
amqp = AMQP::Client.new("amqp://localhost").start

# Declares a durable queue
myqueue = amqp.queue("myqueue")

# Bind the queue to any exchange, with any binding key
myqueue.bind("amq.topic", "my.events.*")

# The message will be reprocessed if the client loses connection to the broker
# between message arrival and when the message was supposed to be ack'ed.
myqueue.subscribe(prefetch: 20) do |msg|
  process(JSON.parse(msg.body))
  msg.ack
rescue
  msg.reject(requeue: false)
end

# Publish directly to the queue
myqueue.publish({ foo: "bar" }.to_json, content_type: "application/json")

# Publish to any exchange
amqp.publish("my message", "amq.topic", "topic.foo", headers: { foo: 'bar' })
amqp.publish(Zlib.gzip("an event"), "amq.topic", "my.event", content_encoding: 'gzip')
```

## Benchmark

1 byte messages:

| Client | Publish rate | Consume rate | Memory usage |
| ------ | ------------ | ------------ | ------------ |
| amqp-client.rb | 237.000 msgs/s | 154.000 msgs/s | 23 MB |
| bunny | 39.000 msgs/s | 44.000 msgs/s | 31 MB |

Gem comparison:

| Client | Runtime dependencies | [Lines of code](https://github.com/AlDanial/cloc) |
| --- | --- | --- |
| amqp-client.rb | 0 | 1876 |
| bunny | 2 | 4003 |

## Supported Ruby versions

All maintained Ruby versions are supported.

- 3.1
- 3.0
- 2.7
- 2.6
- jruby
- truffleruby

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'amqp-client'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install amqp-client

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at [https://github.com/cloudamqp/amqp-client.rb](https://github.com/cloudamqp/amqp-client.rb/)

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
