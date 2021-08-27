## [Unreleased]

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
