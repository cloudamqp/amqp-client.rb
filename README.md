# AMQP::Client

An AMQP 0-9-1 client alternative, trying to keep things as simple as possible.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'amqp-client'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install amqp-client

## Usage

Low level API

```ruby
require "amqp-client"

c = AMQP::Client.new("amqp://guest:guest@localhost")
conn = c.connect
ch = conn.channel
q = ch.queue_declare
ch.basic_publish "Hello World!", "", q[:queue_name]
msg = ch.basic_get q[:queue_name]
puts msg.body
```

High level API, is an easier and safer API, that only deal with durable queues and persisted messages. All methods are blocking in the case of connection loss etc. It's also fully thread-safe. Don't expect it to be extreme throughput, be expect 100% delivery guarantees (messages might be deliviered twice, in the unlikely event of a connection loss between message publish and message confirmed by the server).

```ruby
amqp = AMQP::Client.new("amqp://localhost")
amqp.start

# Declares a durable queue
q = amqp.queue("myqueue")

# Bind the queue to any exchange, with any binding key
q.bind("amq.topic", "my.events.*")

# The message will be reprocessed if the client lost connection to the server
# between the message arrived and the message was supposed to be ack:ed.
q.subscribe(prefetch: 20) do |msg|
  process(JSON.parse(msg.body))
  msg.ack
rescue
  msg.reject(requeue: false)
end

# Publish directly to the queue
q.publish { foo: "bar" }.to_json, content_type: "application/json"

# Publish to any exchange
amqp.publish("my message", "amq.topic", "topic.foo", headers: { foo: 'bar' })
amqp.publish(Zlib.gzip("an event"), "amq.topic", "my.event", content_encoding: 'gzip')
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/cloudamqp/amqp-client.rb

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
