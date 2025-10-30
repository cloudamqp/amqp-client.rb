# Migration Guide: v1.x to v2.0

This guide will help you migrate your code from amqp-client v1.x to v2.0. Version 2.0 introduces several breaking changes that improve API consistency, clarity, and functionality.

## Table of Contents

- [Breaking Changes](#breaking-changes)
  - [1. Keyword Arguments](#1-keyword-arguments)
  - [2. Exchange Convenience Methods Renamed](#2-exchange-convenience-methods-renamed)
  - [3. Direct Exchange Default Name](#3-direct-exchange-default-name)
  - [4. Subscribe Methods Return Consumer](#4-subscribe-methods-return-consumer)
  - [5. Channel#basic_subscribe Return Value](#5-channelbasic_subscribe-return-value)
  - [6. QueueOk Changed to Data Class](#6-queueok-changed-to-data-class)
- [New Features](#new-features)
  - [Automatic Message Encoding/Serialization](#automatic-message-encodingserialization)
  - [Default Content Type and Encoding](#default-content-type-and-encoding)
  - [Automatic Ack/Reject in Queue#subscribe](#automatic-ackreject-in-queuesubscribe)
  - [RPC API](#rpc-api)
  - [Queue Polling](#queue-polling)

## Breaking Changes

### 1. Keyword Arguments

**Impact: ALL public API methods**

All public methods now use keyword arguments instead of positional arguments for improved clarity and to prevent argument order mistakes.

#### High-Level API Changes

**Publishing:**

```ruby
# v1.x
amqp.publish("my message", "amq.topic", "routing.key", headers: { foo: "bar" })
queue.publish("body", content_type: "application/json")
exchange.publish("body", "routing.key")

# v2.0
amqp.publish("my message", exchange: "amq.topic", routing_key: "routing.key", headers: { foo: "bar" })
queue.publish("body", content_type: "application/json")
exchange.publish("body", routing_key: "routing.key")
```

**Exchange Declaration:**

```ruby
# v1.x
amqp.exchange("my.exchange", "x-consistent-hash", durable: true)

# v2.0
amqp.exchange("my.exchange", type: "x-consistent-hash", durable: true)
```

**Queue Binding:**

```ruby
# v1.x
queue.bind("amq.topic", "routing.key")

# v2.0
queue.bind("amq.topic", binding_key: "routing.key")
# Or better, use the exchange object:
exchange = amqp.topic_exchange("amq.topic")
queue.bind(exchange, binding_key: "routing.key")
```

#### Low-Level API Changes

**Publishing:**

```ruby
# v1.x
channel.basic_publish("body", "exchange", "routing.key", persistent: true)
channel.basic_publish_confirm("body", "exchange", "routing.key", persistent: true)

# v2.0
channel.basic_publish("body", exchange: "exchange", routing_key: "routing.key", persistent: true)
channel.basic_publish_confirm("body", exchange: "exchange", routing_key: "routing.key", persistent: true)
```

**Exchange Declaration:**

```ruby
# v1.x
channel.exchange_declare("my.exchange", "topic", durable: true)

# v2.0
channel.exchange_declare("my.exchange", type: "topic", durable: true)
```

### 2. Exchange Convenience Methods Renamed

**Impact: Code using convenience methods for exchange types**

Exchange convenience methods have been renamed to include `_exchange` suffix for improved clarity:

```ruby
# v1.x
amqp.direct("my.exchange")
amqp.fanout("my.exchange")
amqp.topic("my.exchange")
amqp.headers("my.exchange")

# v2.0
amqp.direct_exchange("my.exchange")
amqp.fanout_exchange("my.exchange")
amqp.topic_exchange("my.exchange")
amqp.headers_exchange("my.exchange")
```

### 3. Direct Exchange Default Name

**Impact: Code relying on the direct exchange method to get the default exchange**

The default name for the direct exchange has changed from an empty string (the default exchange) to `"amq.direct"` for API consistency:

```ruby
# v1.x
amqp.direct()         # Returns the default exchange
amqp.direct("")       # Returns the default exchange

# v2.0
amqp.direct_exchange()        # Returns exchange with name "amq.direct"
amqp.direct_exchange("")      # Returns the default exchange
```

**Migration:**

- If you were relying on `direct()` to return the default exchange (empty string), use `direct_exchange("")` explicitly
- You should probably use `default_exchange` rather than `direct_exchange("")`.*

### 4. Subscribe Methods Return Consumer

**Impact: Code using Client#subscribe or Queue#subscribe**

The `subscribe` methods now return a `Consumer` object which can be used to cancel the subscription:

```ruby
# v1.x
queue.subscribe(prefetch: 10) do |msg|
  puts msg.body
end
# No way to cancel the subscription

# v2.0
consumer = queue.subscribe(prefetch: 10) do |msg|
  puts msg.body
end

# Can now cancel:
consumer.cancel
```

**Migration:**

- If you don't need to cancel subscriptions, you can ignore the return value
- To cancel a subscription, store the returned `Consumer` object and call `cancel` on it

### 5. Channel#basic_subscribe Return Value

**Impact: Low-level API code using basic_subscribe**

The `Channel#basic_subscribe` method now returns `Connection::Channel::ConsumeOk` for better consumer response handling:

```ruby
# v1.x
channel.basic_consume(queue_name, no_ack: false) do |msg|
  # ...
end
# Returned some internal value

# v2.0
consume_ok = channel.basic_consume(queue_name, no_ack: false) do |msg|
  # ...
end
# consume_ok has: consumer_tag
```

**Migration:**

- If you weren't using the return value, no changes needed
- If you were using it, update to use the `ConsumeOk` structure

### 6. QueueOk Changed to Data Class

**Impact: Code inspecting QueueOk structure internals**

`Connection::Channel::QueueOk` has been converted from a `Struct` to a `Data` class (immutable):

```ruby
# v1.x
queue_ok = channel.queue_declare("my.queue")
queue_ok.queue_name = "something else"  # Mutable

# v2.0
queue_ok = channel.queue_declare(name: "my.queue")
queue_ok.queue_name = "something else"  # Error: Data objects are immutable
```

**Migration:**

- If you were only reading fields, no changes needed
- If you were modifying the structure, you'll need to create new instances instead

## New Features

While not breaking changes, these new features may allow you to simplify your code:

### Configure Block Pattern

A new unified configuration API has been introduced:

```ruby
AMQP::Client.configure do |config|
  config.enable_builtin_codecs
  config.default_content_type = "application/json"
  config.default_content_encoding = "gzip"
  config.strict_coding = true

  # Can also register custom parsers/coders
  config.register_parser(content_type: "application/msgpack", parser: MsgPackParser)
end
```

### Automatic Message Encoding/Serialization

The high-level API now supports automatic message encoding and serialization:

```ruby
# Enable built-in codecs via configure block
AMQP::Client.configure do |config|
  config.enable_builtin_codecs
end

# Automatically serializes to JSON
queue.publish({ foo: "bar" }, content_type: "application/json")

# Automatically deserializes based on content_type
queue.subscribe do |msg|
  data = msg.parse  # Returns the parsed Ruby hash
end
```

**Supported formats:**

- `application/json` - JSON encoding/decoding
- `gzip` - Gzip compression
- `deflate` - Deflate compression

### Default Content Type and Encoding

You can set default `content_type` and `content_encoding` in the configure block:

```ruby
# Class-level defaults via configure block
AMQP::Client.configure do |config|
  config.default_content_type = "application/json"
  config.default_content_encoding = "gzip"
end

# Instance-level override
amqp = AMQP::Client.new("amqp://localhost")
amqp.default_content_type = "text/plain"

# These will be applied automatically unless explicitly overridden
queue.publish({ foo: "bar" })  # Automatically uses application/json
```

### Automatic Ack/Reject in Queue#subscribe

`Queue#subscribe` now handles acknowledgments and rejections automatically:

```ruby
# Automatic handling
queue.subscribe(prefetch: 20) do |msg|
  process(msg)
  # Message is automatically ack'd if block returns successfully
  # Message is automatically rejected (with requeue) if block raises an exception
end

# Manual handling still works if you prefer
queue.subscribe do |msg|
  msg.ack
rescue => e
  msg.reject(requeue: false)
end
```

### RPC API

A new RPC API has been added for request-response patterns:

```ruby
# Server
amqp.rpc_server("rpc_queue") do |request|
  { result: request[:value] * 2 }
end

# Client
rpc = amqp.rpc_client
response = rpc.call({ value: 21 }, routing_key: "rpc_queue")
# => { result: 42 }
```

### Queue Polling

You can now poll messages from queues as an alternative to subscribing:

```ruby
# Get a single message (returns nil if queue is empty)
msg = queue.get

# Or using the client
msg = amqp.get(queue: "my.queue")

if msg
  process(msg)
  msg.ack
end
```

**New parameters:**

- `passive` - Check if queue exists without creating it
- `exclusive` - Queue will be deleted when connection closes

## Migration Checklist

1. [ ] Update all `publish` calls to use keyword arguments
2. [ ] Update all `exchange_declare` calls to use `type:` keyword
3. [ ] Update all `queue_declare` calls to use keyword arguments
4. [ ] Rename `direct()`, `fanout()`, `topic()`, `headers()` to `*_exchange()` variants
5. [ ] Review uses of `direct()` with no arguments - may need to explicitly pass `""`
6. [ ] If cancelling subscriptions, store the returned `Consumer` object
7. [ ] Update any code that was mutating `QueueOk` structures
8. [ ] Replace `require "amqp-client/enable_builtin_codecs"` with configure block
9. [ ] Consider using automatic message encoding for JSON/gzip/deflate
10. [ ] Consider using automatic ack/reject in `Queue#subscribe`
11. [ ] Review and test all queue bindings to use the new syntax

## Getting Help

If you encounter issues during migration:

1. Check the [API documentation](https://cloudamqp.github.io/amqp-client.rb/)
2. Review the [examples in the README](https://github.com/cloudamqp/amqp-client.rb#readme)
3. [Open an issue](https://github.com/cloudamqp/amqp-client.rb/issues) on GitHub
4. [Contact CloudAMQP support](mailto:support@cloudamqp.com)
