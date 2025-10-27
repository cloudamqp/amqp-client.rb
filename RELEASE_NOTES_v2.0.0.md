# AMQP Client 2.0.0

We're excited to announce the release of AMQP Client 2.0! This major version brings significant improvements to API clarity, consistency, and functionality while introducing powerful new features for automatic message encoding and configuration management.

## 🎯 Highlights

- **Keyword Arguments Throughout** - All public methods now use keyword arguments for improved clarity and safety
- **Unified Configuration API** - New `AMQP::Client.configure` block for centralized configuration
- **Automatic Message Encoding** - Built-in support for JSON, gzip, and deflate with extensible codec registry
- **RPC Support** - Native RPC client/server implementation
- **Enhanced Consumer Control** - Subscribe methods now return cancelable `Consumer` objects
- **Automatic Ack/Reject** - `Queue#subscribe` handles acknowledgments automatically

## 💔 Breaking Changes

### 1. Keyword Arguments (All APIs)

All public methods now require keyword arguments instead of positional arguments:

```ruby
# v1.x
amqp.publish("message", "amq.topic", "routing.key")

# v2.0
amqp.publish("message", exchange: "amq.topic", routing_key: "routing.key")
```

**Migration:** Update all method calls to use keyword arguments. See the [Migration Guide](MIGRATION_GUIDE_v2.md#1-keyword-arguments) for complete examples.

### 2. Exchange Convenience Methods Renamed

Exchange type methods now have an `_exchange` suffix for clarity:

```ruby
# v1.x
amqp.direct()
amqp.topic()
amqp.fanout()

# v2.0
amqp.direct_exchange()
amqp.topic_exchange()
amqp.fanout_exchange()
```

### 3. Direct Exchange Default Name

The default direct exchange name changed from `""` to `"amq.direct"` for consistency:

```ruby
# v1.x
amqp.direct()  # Returns exchange with name ""

# v2.0
amqp.direct_exchange()     # Returns "amq.direct"
amqp.direct_exchange("")   # Explicitly use empty string if needed
```

### 4. Subscribe Returns Consumer

`Client#subscribe` and `Queue#subscribe` now return a `Consumer` object that can be cancelled:

```ruby
consumer = queue.subscribe(prefetch: 10) do |msg|
  # Process message
end

# Later...
consumer.cancel
```

### 5. Other Breaking Changes

- `Channel#basic_subscribe` now returns `Connection::Channel::ConsumeOk`
- `Connection::Channel::QueueOk` is now an immutable `Data` class instead of `Struct`

## ✨ New Features

### Unified Configuration

Configure all settings in one place with the new configure block:

```ruby
AMQP::Client.configure do |config|
  config.enable_builtin_codecs  # JSON, gzip, deflate
  config.default_content_type = "application/json"
  config.default_content_encoding = "gzip"
  config.strict_coding = true

  # Register custom codecs
  config.register_parser(content_type: "application/msgpack", parser: MsgPackParser)
end
```

**Note:** The old `require "amqp-client/enable_builtin_codecs"` approach has been removed.

### Automatic Message Encoding

Built-in support for JSON serialization and compression:

```ruby
# Enable built-in codecs
AMQP::Client.configure do |config|
  config.enable_builtin_codecs
end

# Automatically serializes to JSON
queue.publish({ user: "john", action: "login" }, content_type: "application/json")

# Automatically deserializes
queue.subscribe do |msg|
  data = msg.parse  # Returns Ruby hash
end
```

**Supported formats:**
- `application/json` - JSON encoding/decoding
- `gzip` - Gzip compression
- `deflate` - Deflate compression

### Automatic Ack/Reject

`Queue#subscribe` now handles message acknowledgments automatically:

```ruby
queue.subscribe(prefetch: 20) do |msg|
  process(msg)
  # Automatically ack'd on success
  # Automatically rejected (with requeue) on exception
end
```

Manual control is still available if needed.

### RPC Support

Native RPC implementation for request-response patterns:

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

Poll messages without subscribing:

```ruby
msg = queue.get
if msg
  process(msg)
  msg.ack
end
```

### Enhanced Queue Options

- `passive` - Check queue existence without creating
- `exclusive` - Delete queue when connection closes

### Improved Consumer Management

- `Client#started?` - Check if client is started
- `Message#delivery_info` - Structured access to delivery metadata
- Consumer objects can be cancelled

## 🔧 Additional Improvements

- **Thread Safety** - `Client#start` is now thread-safe
- **Confirm Handling** - `wait_for_confirms` properly handles NACKs and returns boolean
- **Network Optimization** - Nagle's algorithm disabled for lower latency
- **Bug Fixes** - Various fixes including minitest compatibility

## 📚 Migration

Please review the complete [Migration Guide](MIGRATION_GUIDE_v2.md) for detailed migration instructions, code examples, and a step-by-step checklist.

### Quick Migration Checklist

1. ✅ Update all method calls to use keyword arguments
2. ✅ Rename `direct()`, `fanout()`, `topic()`, `headers()` to `*_exchange()`
3. ✅ Replace `require "amqp-client/enable_builtin_codecs"` with configure block
4. ✅ Store returned `Consumer` objects if you need to cancel subscriptions
5. ✅ Review direct exchange usage if relying on empty string default
6. ✅ Update any code mutating `QueueOk` structures

### Migration Support

- 📖 [Full Migration Guide](MIGRATION_GUIDE_v2.md)
- 📚 [API Documentation](https://cloudamqp.github.io/amqp-client.rb/)
- 💬 [Open an Issue](https://github.com/cloudamqp/amqp-client.rb/issues)
- 📧 [Contact Support](mailto:support@cloudamqp.com)

## 🙏 Acknowledgments

Thank you to everyone who contributed feedback, bug reports, and suggestions that shaped this release!

---

**Full Changelog**: https://github.com/cloudamqp/amqp-client.rb/compare/v1.2.1...v2.0.0
