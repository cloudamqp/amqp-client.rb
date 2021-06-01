# frozen_string_literal: true

require_relative "./table"

module AMQP
  # Encode/decode AMQP Properties
  module Properties
    module_function

    def encode(hash)
      flags = 0
      arr = [flags]
      fmt = String.new("S>")

      if (content_type = hash[:content_type])
        flags |= (1 << 15)
        arr << content_type.bytesize << content_type
        fmt << "Ca*"
      end

      if (content_encoding = hash[:content_encoding])
        flags |= (1 << 14)
        arr << content_encoding.bytesize << content_encoding
        fmt << "Ca*"
      end

      if (headers = hash[:headers])
        flags |= (1 << 13)
        tbl = Table.encode(headers)
        arr << tbl.bytesize << tbl
        fmt << "L>a*"
      end

      if (delivery_mode = hash[:delivery_mode])
        flags |= (1 << 12)
        arr << delivery_mode
        fmt << "C"
      end

      if (priority = hash[:priority])
        flags |= (1 << 11)
        arr << priority
        fmt << "C"
      end

      if (correlation_id = hash[:correlation_id])
        flags |= (1 << 10)
        arr << correlation_id.bytesize << correlation_id
        fmt << "Ca*"
      end

      if (reply_to = hash[:reply_to])
        flags |= (1 << 9)
        arr << reply_to.bytesize << reply_to
        fmt << "Ca*"
      end

      if (expiration = hash[:expiration])
        flags |= (1 << 8)
        arr << expiration.bytesize << expiration
        fmt << "Ca*"
      end

      if (message_id = hash[:message_id])
        flags |= (1 << 7)
        arr << message_id.bytesize << message_id
        fmt << "Ca*"
      end

      if (timestamp = hash[:timestamp])
        flags |= (1 << 6)
        arr << timestamp.to_i
        fmt << "Q>"
      end

      if (type = hash[:type])
        flags |= (1 << 5)
        arr << type.bytesize << type
        fmt << "Ca*"
      end

      if (user_id = hash[:user_id])
        flags |= (1 << 4)
        arr << user_id.bytesize << user_id
        fmt << "Ca*"
      end

      if (app_id = hash[:app_id])
        flags |= (1 << 3)
        arr << app_id.bytesize << app_id
        fmt << "Ca*"
      end

      arr[0] = flags
      arr.pack(fmt)
    end

    def decode(bytes)
      h = {}
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
