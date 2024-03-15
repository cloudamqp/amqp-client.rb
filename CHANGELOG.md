## [Unreleased]

## [1.1.5] - 2024-03-15

- Fixed: Correctly reference the `UnexpectedFrameEnd` exception

## [1.1.4] - 2021-12-27

- Fixed: Ruby 3.1.0 compability, StringIO have to be required manually

## [1.1.3] - 2021-11-04

- Fixed: Reraise SystemcallError in connect so that reconnect works
- Fixed: Keepalive support in OS X
- Added: Make keepalive settings configurable (eg. amqp://?keepalive=60:10:3)

## [1.1.2] - 2021-10-15

- Added: Support for JRuby and TruffleRuby

## [1.1.1] - 2021-09-15

- Added: Examples in the documentation
- Added: Faster Properties and Table encoding and decoding

## [1.1.0] - 2021-09-08

- Fixed: Due to a race condition publishers could get stuck waiting for publish confirms
- Change: Message, ReturnMessage and Properties are now classes and not structs (for performance reasons)
- Added: Ruby 2.6 support
- Added: RBS signatures in sig/amqp-client.rbs

## [1.0.2] - 2021-09-07

- Changed: Raise ConnectionClosed and ChannelClosed correctly (previous always ChannelClosed)
- Fixed: Respect Connection#blocked sent by the broker, will block all writes/requests

## [1.0.1] - 2021-09-06

- The API is fully documented! https://cloudamqp.github.io/amqp-client.rb/
- Fixed: Socket writing is now thread-safe
- Change: Block while waiting for basic_cancel by default
- Added: Can specify channel_max, heartbeat and frame_max as options to the Client/Connection
- Added: Reuse channel 1 to declare high level queues/exchanges
- Fixed: Only wait for exchange_delete confirmation if not no_wait is set
- Fixed: Don't raise if Connection#close detects a closed socket (expected)

## [1.0.0] - 2021-08-27

- Verify TLS certificate matches hostname
- TLS thread-safety
- Assemble Messages in the (single threaded) read_loop thread
- Give read_loop_thread higher priority so that channel errors crop up faster
- One less Thread required per Consumer
- Read exactly one frame at a time, not trying to split/assemble frames over socket reads
- Heafty speedup for message assembling with StringIO
- Channel#queue_declare returns a struct for nicer API (still backward compatible)
- AMQP::Client#publish_and_forget for fast, non confirmed publishes
- Allow Properties#timestamp to be an integer (in addition to Time)
- Bug fix allow Properties#expiration to be an Integer
- Consistent use of named parameters
- High level Exchange API
- Don't try to reconnect if first connect fails
- Bug fix: Close all channels when connection is closed by server
- Raise error if run out of channels
- Improved retry in high level client
- Bug fix: Support channel_max 0

## [0.3.0] - 2021-08-20

- Channel#wait_for_confirms is a smarter way of waiting for publish confirms
- Default connection_name to $PROGRAM_NAME

## [0.2.3] - 2021-08-19

- Improved TLS/AMQPS support

## [0.2.2] - 2021-08-19

- TLS port issue fixed

## [0.2.1] - 2021-08-19

- More arguments to be passed to AMQP::Client::Queue
- Can require with 'amqp-client'

## [0.2.0] - 2021-08-19

- Much improved and with a high level client

## [0.1.0] - 2021-04-13

- Initial release
