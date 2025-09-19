# AMQP::Client

A modern AMQP 0-9-1 Ruby client. Very fast (just as fast as the Java client, and >4x than other Ruby clients), fully thread-safe, blocking operations and straight-forward error handling.

It's small, only ~1800 lines of code, and without any dependencies. Other Ruby clients are about 4 times bigger. But without trading functionallity.

It's safe by default, messages are published as persistent, and is waiting for confirmation from the broker. That can of course be disabled if performance is a priority.

## Support

The library is fully supported by [CloudAMQP](https://www.cloudamqp.com), the largest LavinMQ and RabbitMQ hosting provider in the world. Open [an issue](https://github.com/cloudamqp/amqp-client.rb/issues) or [email our support](mailto:support@cloudamqp.com) if you have problems or questions.

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

The library provides a high-level API that manages channels, content-types, encodings, reconnection automatically.

```ruby
require "amqp-client"
require "json"
require "zlib"

# Start the client, it will connect and once connected it will reconnect if that connection is lost
# Operation pending when the connection is lost will raise an exception (not timeout)
amqp = AMQP::Client.new("amqp://localhost").start

# Declares a durable queue
myqueue = amqp.queue("myqueue")

# Declares a topic exchange
ex = amqp.topic_exchange("myexchange")

# Bind the queue to any exchange, with any binding key
myqueue.bind(ex, "my.events.*")

# The message will be reprocessed if the client loses connection to the broker
# between message arrival and when the message was supposed to be ack'ed.
myqueue.subscribe(prefetch: 20) do |msg|
  puts JSON.parse(msg.body)
rescue => e
  puts e.full_message
end
# The message is automatically ack'd by Queue#subscribe if the block returns successfully
# If the block raises the message is rejected and requeued.
# You still can control the acking and rejecting in the block if you want to, e.g:
myqueue.subscribe do |msg|
  msg.ack
rescue => e
  msg.reject
end

# Publish directly to the queue
myqueue.publish({ foo: "bar" }.to_json, content_type: "application/json")

# Publish to any exchange
ex.publish("my message", "topic.foo", headers: { foo: "bar" })
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

See the [CI workflow](https://github.com/cloudamqp/amqp-client.rb/blob/main/.github/workflows/main.yml) for the exact versions.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'amqp-client'
```

And then execute:

```bash
    bundle install
```

Or install it yourself as:

```bash
    gem install amqp-client
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`.

### Release Process

The gem uses rake tasks to automate the release preparation process. The actual gem building and publishing is handled automatically by GitHub Actions when a tag is pushed.

#### Quick Release (Patch Version)

```bash
rake release:prepare
```

This will:

1. Run tests and RuboCop to ensure code quality
2. Bump the patch version (e.g., 1.2.0 → 1.2.1)
3. Update the CHANGELOG.md with the new version and current date
4. Create a git commit and tag for the release
5. Push commits and tags to the remote repository
6. GitHub Actions will automatically build and publish the gem to RubyGems

#### Custom Version Bump

For minor or major version bumps:

```bash
# Minor version bump (e.g., 1.2.0 → 1.3.0)
rake release:prepare[minor]

# Major version bump (e.g., 1.2.0 → 2.0.0)
rake release:prepare[major]
```

#### Individual Release Steps

You can also run individual steps if needed:

```bash
# Bump version only
rake release:bump[patch]  # or [minor] or [major]

# Update changelog with current version
rake release:changelog

# Create git tag with changelog entries
rake release:tag

# Push tag to remote (handles conflicts)
rake release:push_tag
```

#### Manual Release Steps

If you prefer manual control:

1. Update the version number in `lib/amqp/client/version.rb`
2. Update the CHANGELOG.md with the new version and release notes
3. Commit your changes: `git add . && git commit -m "Release X.Y.Z"`
4. Create and push a tag: `git tag vX.Y.Z && git push origin vX.Y.Z`
5. GitHub Actions will automatically build and publish the gem when the tag is pushed

## Contributing

Bug reports and pull requests are welcome on GitHub at [https://github.com/cloudamqp/amqp-client.rb](https://github.com/cloudamqp/amqp-client.rb/)

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
