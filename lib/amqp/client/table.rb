# frozen_string_literal: true

module AMQP
  class Client
    # Encode and decode AMQP Tables to/from hash
    module Table
      module_function

      # Encodes a hash into a byte array
      def encode(hash)
        tbl = StringIO.new
        hash.each do |k, v|
          key = k.to_s
          tbl.write [key.bytesize, key].pack("Ca*")
          tbl.write encode_field(v)
        end
        tbl.string
      end

      # Decodes an AMQP table into a hash
      def decode(bytes)
        hash = {}
        pos = 0
        while pos < bytes.bytesize
          key_len = bytes[pos].ord
          pos += 1
          key = bytes.byteslice(pos, key_len).force_encoding("utf-8")
          pos += key_len
          rest = bytes.byteslice(pos, bytes.bytesize - pos)
          len, value = decode_field(rest)
          pos += len + 1
          hash[key] = value
        end
        hash
      end

      def encode_field(value)
        case value
        when Integer
          if value > 2**31
            ["l", value].pack("a q>")
          else
            ["I", value].pack("a l>")
          end
        when Float
          ["d", value].pack("a G")
        when String
          ["S", value.bytesize, value].pack("a L> a*")
        when Time
          ["T", value.to_i].pack("a Q>")
        when Array
          bytes = value.map { |e| encode_field(e) }.join
          ["A", bytes.bytesize, bytes].pack("a L> a*")
        when Hash
          bytes = Table.encode(value)
          ["F", bytes.bytesize, bytes].pack("a L> a*")
        when true
          ["t", 1].pack("a C")
        when false
          ["t", 0].pack("a C")
        when nil
          ["V"].pack("a")
        else raise "unsupported table field type: #{value.class}"
        end
      end

      # returns [length of field including type, value of field]
      def decode_field(bytes)
        type = bytes[0]
        pos = 1
        case type
        when "S"
          len = bytes.byteslice(pos, 4).unpack1("L>")
          pos += 4
          [4 + len, bytes.byteslice(pos, len).force_encoding("utf-8")]
        when "F"
          len = bytes.byteslice(pos, 4).unpack1("L>")
          pos += 4
          [4 + len, decode(bytes.byteslice(pos, len))]
        when "A"
          len = bytes.byteslice(pos, 4).unpack1("L>")
          a = []
          while pos < len
            length, value = decode_field(bytes.byteslice(pos, -1))
            pos += length + 1
            a << value
          end
          [4 + len, a]
        when "t"
          [1, bytes[pos].ord == 1]
        when "b"
          [1, bytes.byteslice(pos, 1).unpack1("c")]
        when "B"
          [1, bytes.byteslice(pos, 1).unpack1("C")]
        when "s"
          [2, bytes.byteslice(pos, 2).unpack1("s")]
        when "u"
          [2, bytes.byteslice(pos, 2).unpack1("S")]
        when "I"
          [4, bytes.byteslice(pos, 4).unpack1("l>")]
        when "i"
          [4, bytes.byteslice(pos, 4).unpack1("L>")]
        when "l"
          [8, bytes.byteslice(pos, 8).unpack1("q>")]
        when "f"
          [4, bytes.byteslice(pos, 4).unpack1("g")]
        when "d"
          [8, bytes.byteslice(pos, 8).unpack1("G")]
        when "D"
          scale = bytes[pos].ord
          pos += 1
          value = bytes.byteslice(pos, 4).unpack1("L>")
          d = value / 10**scale
          [5, d]
        when "x"
          len = bytes.byteslice(pos, 4).unpack1("L>")
          [4 + len, bytes.byteslice(pos, len)]
        when "T"
          [8, Time.at(bytes.byteslice(pos, 8).unpack1("Q>"))]
        when "V"
          [0, nil]
        else raise "unsupported table field type: #{type}"
        end
      end
    end
  end
end
