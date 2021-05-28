module AMQP
  class Client
    class Error < StandardError; end

    # Raised when a frame that wasn't expected arrives
    class UnexpectedFrame < Error
      def initialize(expected, actual)
        super "Expected frame type '#{expected}' but got '#{actual}'"
      end
    end

    # Raised when a frame doesn't end with 206
    class UnexpectedFrameEnd < Error
      def initialize(actual)
        super "Expected frame end 206 but got '#{actual}'"
      end
    end

    class UnsupportedFrameType < Error
      def initialize(type)
        super "Unsupported frame type '#{type}'"
      end
    end

    class UnsupportedMethodFrame < Error
      def initialize(class_id, method_id)
        super "Unsupported class/method: #{class_id} #{method_id}"
      end
    end

    class ChannelClosedError < Error
      def initialize(id)
        super "Channel #{id} already closed"
      end
    end
  end
end
