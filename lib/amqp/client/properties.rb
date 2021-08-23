# frozen_string_literal: true

require_relative "./table"

module AMQP
  # Encode/decode AMQP Properties
  Properties = Struct.new(:content_type, :content_encoding, :headers, :delivery_mode, :priority, :correlation_id,
                          :reply_to, :expiration, :message_id, :timestamp, :type, :user_id, :app_id,
                          keyword_init: true) do
    def encode
      flags = 0
      arr = [flags]
      fmt = String.new("S>")

      if content_type
        content_type.is_a?(String) || raise(ArgumentError, "content_type must be a string")

        flags |= (1 << 15)
        arr << content_type.bytesize << content_type
        fmt << "Ca*"
      end

      if content_encoding
        content_encoding.is_a?(String) || raise(ArgumentError, "content_encoding must be a string")

        flags |= (1 << 14)
        arr << content_encoding.bytesize << content_encoding
        fmt << "Ca*"
      end

      if headers
        headers.is_a?(Hash) || raise(ArgumentError, "headers must be a hash")

        flags |= (1 << 13)
        tbl = Table.encode(headers)
        arr << tbl.bytesize << tbl
        fmt << "L>a*"
      end

      if delivery_mode
        delivery_mode.is_a?(Integer) || raise(ArgumentError, "delivery_mode must be an int")
        delivery_mode.between?(0, 2) || raise(ArgumentError, "delivery_mode must be be between 0 and 2")

        flags |= (1 << 12)
        arr << delivery_mode
        fmt << "C"
      end

      if priority
        priority.is_a?(Integer) || raise(ArgumentError, "priority must be an int")
        flags |= (1 << 11)
        arr << priority
        fmt << "C"
      end

      if correlation_id
        priority.is_a?(String) || raise(ArgumentError, "correlation_id must be a string")

        flags |= (1 << 10)
        arr << correlation_id.bytesize << correlation_id
        fmt << "Ca*"
      end

      if reply_to
        reply_to.is_a?(String) || raise(ArgumentError, "reply_to must be a string")

        flags |= (1 << 9)
        arr << reply_to.bytesize << reply_to
        fmt << "Ca*"
      end

      if expiration
        self.expiration = expiration.to_s if expiration.is_a?(Integer)
        expiration.is_a?(String) || raise(ArgumentError, "expiration must be a string or integer")

        flags |= (1 << 8)
        arr << expiration.bytesize << expiration
        fmt << "Ca*"
      end

      if message_id
        message_id.is_a?(String) || raise(ArgumentError, "message_id must be a string")

        flags |= (1 << 7)
        arr << message_id.bytesize << message_id
        fmt << "Ca*"
      end

      if timestamp
        timestamp.is_a?(Integer) || timestamp.is_a?(Time) || raise(ArgumentError, "timestamp must be an Integer or a Time")

        flags |= (1 << 6)
        arr << timestamp.to_i
        fmt << "Q>"
      end

      if type
        type.is_a?(String) || raise(ArgumentError, "type must be a string")

        flags |= (1 << 5)
        arr << type.bytesize << type
        fmt << "Ca*"
      end

      if user_id
        user_id.is_a?(String) || raise(ArgumentError, "user_id must be a string")

        flags |= (1 << 4)
        arr << user_id.bytesize << user_id
        fmt << "Ca*"
      end

      if app_id
        app_id.is_a?(String) || raise(ArgumentError, "app_id must be a string")

        flags |= (1 << 3)
        arr << app_id.bytesize << app_id
        fmt << "Ca*"
      end

      arr[0] = flags
      arr.pack(fmt)
    end

    def self.decode(bytes)
      h = new
      flags = bytes.unpack1("S>")
      pos = 2
      if (flags & 0x8000).positive?
        len = bytes[pos].ord
        pos += 1
        h[:content_type] = bytes.byteslice(pos, len).force_encoding("utf-8")
        pos += len
      end
      if (flags & 0x4000).positive?
        len = bytes[pos].ord
        pos += 1
        h[:content_encoding] = bytes.byteslice(pos, len).force_encoding("utf-8")
        pos += len
      end
      if (flags & 0x2000).positive?
        len = bytes.byteslice(pos, 4).unpack1("L>")
        pos += 4
        h[:headers] = Table.decode(bytes.byteslice(pos, len))
        pos += len
      end
      if (flags & 0x1000).positive?
        h[:delivery_mode] = bytes[pos].ord
        pos += 1
      end
      if (flags & 0x0800).positive?
        h[:priority] = bytes[pos].ord
        pos += 1
      end
      if (flags & 0x0400).positive?
        len = bytes[pos].ord
        pos += 1
        h[:correlation_id] = bytes.byteslice(pos, len).force_encoding("utf-8")
        pos += len
      end
      if (flags & 0x0200).positive?
        len = bytes[pos].ord
        pos += 1
        h[:reply_to] = bytes.byteslice(pos, len).force_encoding("utf-8")
        pos += len
      end
      if (flags & 0x0100).positive?
        len = bytes[pos].ord
        pos += 1
        h[:expiration] = bytes.byteslice(pos, len).force_encoding("utf-8")
        pos += len
      end
      if (flags & 0x0080).positive?
        len = bytes[pos].ord
        pos += 1
        h[:message_id] = bytes.byteslice(pos, len).force_encoding("utf-8")
        pos += len
      end
      if (flags & 0x0040).positive?
        h[:timestamp] = Time.at(bytes.byteslice(pos, 8).unpack1("Q>"))
        pos += 8
      end
      if (flags & 0x0020).positive?
        len = bytes[pos].ord
        pos += 1
        h[:type] = bytes.byteslice(pos, len).force_encoding("utf-8")
        pos += len
      end
      if (flags & 0x0010).positive?
        len = bytes[pos].ord
        pos += 1
        h[:user_id] = bytes.byteslice(pos, len).force_encoding("utf-8")
        pos += len
      end
      if (flags & 0x0008).positive?
        len = bytes[pos].ord
        pos += 1
        h[:app_id] = bytes.byteslice(pos, len).force_encoding("utf-8")
      end
      h
    end
  end
end
