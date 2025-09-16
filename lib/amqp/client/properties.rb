# frozen_string_literal: true

require_relative "table"

module AMQP
  class Client
    # Encode/decode AMQP Properties
    class Properties
      # rubocop:disable Metrics/ParameterLists
      def initialize(content_type: nil, content_encoding: nil, headers: nil, delivery_mode: nil,
                     priority: nil, correlation_id: nil, reply_to: nil, expiration: nil,
                     message_id: nil, timestamp: nil, type: nil, user_id: nil, app_id: nil)
        @content_type = content_type
        @content_encoding = content_encoding
        @headers = headers
        @delivery_mode = delivery_mode
        @priority = priority
        @correlation_id = correlation_id
        @reply_to = reply_to
        @expiration = expiration
        @message_id = message_id
        @timestamp = timestamp
        @type = type
        @user_id = user_id
        @app_id = app_id
      end
      # rubocop:enable Metrics/ParameterLists

      # Properties as a Hash
      # @return [Hash] Properties
      def to_h
        {
          content_type:,
          content_encoding:,
          headers:,
          delivery_mode:,
          priority:,
          correlation_id:,
          reply_to:,
          expiration:,
          message_id:,
          timestamp:,
          type:,
          user_id:,
          app_id:
        }
      end

      alias to_hash to_h

      # Returns true if two Property objects holds the same information
      # @return [Boolean]
      def ==(other)
        return false unless other.is_a? self.class

        instance_variables.all? { |v| instance_variable_get(v) == other.instance_variable_get(v) }
      end

      # Content type of the message body
      # @return [String, nil]
      attr_accessor :content_type
      # Content encoding of the body
      # @return [String, nil]
      attr_accessor :content_encoding
      # Headers, for applications and header exchange routing
      # @return [Hash<String, Object>, nil]
      attr_accessor :headers
      # Message persistent level
      # @note The exchange and queue have to durable as well for the message to be persistent
      # @return [1] Transient message
      # @return [2] Persistent message
      # @return [nil] Not specified (implicitly transient)
      attr_accessor :delivery_mode
      # A priority of the message (between 0 and 255)
      # @return [Integer, nil]
      attr_accessor :priority
      # Message correlation id, commonly used to correlate RPC requests and responses
      # @return [String, nil]
      attr_accessor :correlation_id
      # Queue to reply RPC responses to
      # @return [String, nil]
      attr_accessor :reply_to
      # Number of seconds the message will stay in the queue
      # @return [String, nil]
      attr_accessor :expiration
      # Application message identifier
      # @return [String, nil]
      attr_accessor :message_id
      # Message timestamp, often indicates when the message was originally generated
      # @return [Date, nil]
      attr_accessor :timestamp
      # Message type name
      # @return [String, nil]
      attr_accessor :type
      # The user that published the message
      # @return [String, nil]
      attr_accessor :user_id
      # Name of application that generated the message
      # @return [String, nil]
      attr_accessor :app_id

      # Encode properties into a byte array
      # @param properties [Hash]
      # @return [String] byte array
      def self.encode(properties)
        return "\x00\x00" if properties.empty?

        flags = 0
        arr = [flags]
        fmt = String.new("S>", capacity: 37)

        if (content_type = properties[:content_type])
          content_type.is_a?(String) || raise(ArgumentError, "content_type must be a string")

          flags |= (1 << 15)
          arr << content_type.bytesize << content_type
          fmt << "Ca*"
        end

        if (content_encoding = properties[:content_encoding])
          content_encoding.is_a?(String) || raise(ArgumentError, "content_encoding must be a string")

          flags |= (1 << 14)
          arr << content_encoding.bytesize << content_encoding
          fmt << "Ca*"
        end

        if (headers = properties[:headers])
          headers.is_a?(Hash) || raise(ArgumentError, "headers must be a hash")

          flags |= (1 << 13)
          tbl = Table.encode(headers)
          arr << tbl.bytesize << tbl
          fmt << "L>a*"
        end

        if (delivery_mode = properties[:delivery_mode])
          delivery_mode.is_a?(Integer) || raise(ArgumentError, "delivery_mode must be an int")

          flags |= (1 << 12)
          arr << delivery_mode
          fmt << "C"
        end

        if (priority = properties[:priority])
          priority.is_a?(Integer) || raise(ArgumentError, "priority must be an int")

          flags |= (1 << 11)
          arr << priority
          fmt << "C"
        end

        if (correlation_id = properties[:correlation_id])
          correlation_id.is_a?(String) || raise(ArgumentError, "correlation_id must be a string")

          flags |= (1 << 10)
          arr << correlation_id.bytesize << correlation_id
          fmt << "Ca*"
        end

        if (reply_to = properties[:reply_to])
          reply_to.is_a?(String) || raise(ArgumentError, "reply_to must be a string")

          flags |= (1 << 9)
          arr << reply_to.bytesize << reply_to
          fmt << "Ca*"
        end

        if (expiration = properties[:expiration])
          expiration = expiration.to_s if expiration.is_a?(Integer)
          expiration.is_a?(String) || raise(ArgumentError, "expiration must be a string or integer")

          flags |= (1 << 8)
          arr << expiration.bytesize << expiration
          fmt << "Ca*"
        end

        if (message_id = properties[:message_id])
          message_id.is_a?(String) || raise(ArgumentError, "message_id must be a string")

          flags |= (1 << 7)
          arr << message_id.bytesize << message_id
          fmt << "Ca*"
        end

        if (timestamp = properties[:timestamp])
          timestamp.is_a?(Integer) || timestamp.is_a?(Time) ||
            raise(ArgumentError, "timestamp must be an Integer or a Time")

          flags |= (1 << 6)
          arr << timestamp.to_i
          fmt << "Q>"
        end

        if (type = properties[:type])
          type.is_a?(String) || raise(ArgumentError, "type must be a string")

          flags |= (1 << 5)
          arr << type.bytesize << type
          fmt << "Ca*"
        end

        if (user_id = properties[:user_id])
          user_id.is_a?(String) || raise(ArgumentError, "user_id must be a string")

          flags |= (1 << 4)
          arr << user_id.bytesize << user_id
          fmt << "Ca*"
        end

        if (app_id = properties[:app_id])
          app_id.is_a?(String) || raise(ArgumentError, "app_id must be a string")

          flags |= (1 << 3)
          arr << app_id.bytesize << app_id
          fmt << "Ca*"
        end

        arr[0] = flags
        arr.pack(fmt)
      end

      # Decode a byte array
      # @return [Properties]
      def self.decode(bytes, pos = 0)
        p = new
        flags = bytes.byteslice(pos, 2).unpack1("S>")
        pos += 2
        if (flags & 0x8000).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.content_type = bytes.byteslice(pos, len).force_encoding("utf-8")
          pos += len
        end
        if (flags & 0x4000).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.content_encoding = bytes.byteslice(pos, len).force_encoding("utf-8")
          pos += len
        end
        if (flags & 0x2000).positive?
          len = bytes.byteslice(pos, 4).unpack1("L>")
          pos += 4
          p.headers = Table.decode(bytes.byteslice(pos, len))
          pos += len
        end
        if (flags & 0x1000).positive?
          p.delivery_mode = bytes.getbyte(pos)
          pos += 1
        end
        if (flags & 0x0800).positive?
          p.priority = bytes.getbyte(pos)
          pos += 1
        end
        if (flags & 0x0400).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.correlation_id = bytes.byteslice(pos, len).force_encoding("utf-8")
          pos += len
        end
        if (flags & 0x0200).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.reply_to = bytes.byteslice(pos, len).force_encoding("utf-8")
          pos += len
        end
        if (flags & 0x0100).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.expiration = bytes.byteslice(pos, len).force_encoding("utf-8")
          pos += len
        end
        if (flags & 0x0080).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.message_id = bytes.byteslice(pos, len).force_encoding("utf-8")
          pos += len
        end
        if (flags & 0x0040).positive?
          p.timestamp = Time.at(bytes.byteslice(pos, 8).unpack1("Q>"))
          pos += 8
        end
        if (flags & 0x0020).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.type = bytes.byteslice(pos, len).force_encoding("utf-8")
          pos += len
        end
        if (flags & 0x0010).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.user_id = bytes.byteslice(pos, len).force_encoding("utf-8")
          pos += len
        end
        if (flags & 0x0008).positive?
          len = bytes.getbyte(pos)
          pos += 1
          p.app_id = bytes.byteslice(pos, len).force_encoding("utf-8")
        end
        p
      end
    end
  end
end
