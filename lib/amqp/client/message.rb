# frozen_string_literal: true

module AMQP
  class Client
    # A message delivered from the broker
    class Message
      # Internal API.
      def initialize(channel, consumer_tag, delivery_tag, exchange, routing_key, redelivered)
        @channel = channel
        @consumer_tag = consumer_tag
        @delivery_tag = delivery_tag
        @exchange = exchange
        @routing_key = routing_key
        @redelivered = redelivered
        @properties = nil
        @body = ""
        @ack_or_reject_sent = false
        @parsed = nil
      end

      DeliveryInfo = Struct.new(:consumer_tag, :delivery_tag, :redelivered, :exchange, :routing_key, :channel)

      # The channel the message was deliviered to
      # Returns <tt>Connection::Channel</tt>.
      attr_reader :channel

      # The tag of the consumer the message was deliviered to
      # Returns <tt>String</tt>.
      # Returns <tt>nil</tt> - If the message was polled and not deliviered to a consumer
      attr_reader :consumer_tag

      # The delivery tag of the message, used for acknowledge or reject the message
      # Returns <tt>Integer</tt>.
      attr_reader :delivery_tag

      # Name of the exchange the message was published to
      # Returns <tt>String</tt>.
      attr_reader :exchange

      # The routing key the message was published with
      # Returns <tt>String</tt>.
      attr_reader :routing_key

      # True if the message have been delivered before
      # Returns <tt>Boolean</tt>.
      attr_reader :redelivered

      # Message properties
      # Returns <tt>Properties</tt>.
      attr_accessor :properties

      # The message body
      # Returns <tt>String</tt>.
      attr_accessor :body

      def delivery_info
        @delivery_info ||= DeliveryInfo.new(
          consumer_tag:,
          delivery_tag:,
          redelivered:,
          exchange:,
          routing_key:,
          channel:
        )
      end

      # Acknowledge the message
      # Returns <tt>nil</tt>.
      def ack
        return if @ack_or_reject_sent

        @channel.basic_ack(@delivery_tag)
        @ack_or_reject_sent = true
        nil
      end

      # Reject the message
      # * <tt>requeue</tt> (<tt>Boolean</tt>) - If true the message will be put back into the queue again, ready to be
      #   redelivered
      # Returns <tt>nil</tt>.
      def reject(requeue: false)
        return if @ack_or_reject_sent

        @channel.basic_reject(@delivery_tag, requeue:)
        @ack_or_reject_sent = true
        nil
      end

      # See #exchange.
      # Deprecated.
      # Attribute: <tt>exchange_name</tt>
      # Returns <tt>String</tt>.
      def exchange_name
        @exchange
      end

      # :section: Message coding

      # Parse the message body based on content_type and content_encoding
      # Raises <tt>Error::UnsupportedContentEncoding</tt> - If the content encoding is not supported
      # Raises <tt>Error::UnsupportedContentType</tt> - If the content type is not supported
      # Returns <tt>Object</tt> - The parsed message body
      def parse
        return @parsed unless @parsed.nil?

        registry = @channel.connection.codec_registry
        strict = @channel.connection.strict_coding
        decoded = decode
        ct = @properties&.content_type
        parser = registry&.find_parser(ct)

        return @parsed = parser.parse(decoded, @properties) if parser

        is_unsupported = ct && ct != "" && ct != "text/plain"
        raise Error::UnsupportedContentType, ct if is_unsupported && strict

        @parsed = decoded
      end

      # Decode the message body based on content_encoding
      # Raises <tt>Error::UnsupportedContentEncoding</tt> - If the content encoding is not supported
      # Returns <tt>String</tt> - The decoded message body
      def decode
        registry = @channel.connection.codec_registry
        strict = @channel.connection.strict_coding
        ce = @properties&.content_encoding
        coder = registry&.find_coder(ce)

        return coder.decode(@body, @properties) if coder

        is_unsupported = ce && ce != ""
        raise Error::UnsupportedContentEncoding, ce if is_unsupported && strict

        @body
      end
    end

    # A published message returned by the broker due to some error
    class ReturnMessage
      # Internal API.
      def initialize(reply_code, reply_text, exchange, routing_key)
        @reply_code = reply_code
        @reply_text = reply_text
        @exchange = exchange
        @routing_key = routing_key
        @properties = nil
        @body = ""
      end

      # Error code
      # Returns <tt>Integer</tt>.
      attr_reader :reply_code

      # Description on why the message was returned
      # Returns <tt>String</tt>.
      attr_reader :reply_text

      # Name of the exchange the message was published to
      # Returns <tt>String</tt>.
      attr_reader :exchange

      # The routing key the message was published with
      # Returns <tt>String</tt>.
      attr_reader :routing_key

      # Message properties
      # Returns <tt>Properties</tt>.
      attr_accessor :properties

      # The message body
      # Returns <tt>String</tt>.
      attr_accessor :body
    end
  end
end
