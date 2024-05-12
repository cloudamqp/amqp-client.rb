# frozen_string_literal: true

require_relative "./properties"
require_relative "./table"

module AMQP
  class Client
    # Generate binary data for different frames
    # Each frame type implemented as a method
    # Having a class for each frame type is more expensive in terms of CPU and memory
    # @api private
    module FrameBytes
      def self.connection_start_ok(response, properties)
        prop_tbl = Table.encode(properties)
        [
          1, # type: method
          0, # channel id
          2 + 2 + 4 + prop_tbl.bytesize + 6 + 4 + response.bytesize + 1, # frame size
          10, # class id
          11, # method id
          prop_tbl.bytesize, prop_tbl, # client props
          5, "PLAIN", # mechanism
          response.bytesize, response,
          0, "", # locale
          206 # frame end
        ].pack("C S> L> S> S> L>a* Ca* L>a* Ca* C")
      end

      def self.connection_tune_ok(channel_max, frame_max, heartbeat)
        [
          1, # type: method
          0, # channel id
          12, # frame size
          10, # class: connection
          31, # method: tune-ok
          channel_max,
          frame_max,
          heartbeat,
          206 # frame end
        ].pack("CS>L>S>S>S>L>S>C")
      end

      def self.connection_open(vhost)
        [
          1, # type: method
          0, # channel id
          2 + 2 + 1 + vhost.bytesize + 1 + 1, # frame_size
          10, # class: connection
          40, # method: open
          vhost.bytesize, vhost,
          0, # reserved1
          0, # reserved2
          206 # frame end
        ].pack("C S> L> S> S> Ca* CCC")
      end

      def self.connection_close(code, reason)
        frame_size = 2 + 2 + 2 + 1 + reason.bytesize + 2 + 2
        [
          1, # type: method
          0, # channel id
          frame_size, # frame size
          10, # class: connection
          50, # method: close
          code,
          reason.bytesize, reason,
          0, # error class id
          0, # error method id
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* S> S> C")
      end

      def self.connection_close_ok
        [
          1, # type: method
          0, # channel id
          4, # frame size
          10, # class: connection
          51, # method: close-ok
          206 # frame end
        ].pack("C S> L> S> S> C")
      end

      def self.update_secret(secret, reason)
        frame_size = 4 + 4 + secret.bytesize + 1 + reason.bytesize
        [
          1, # type: method
          0, # channel id
          frame_size, # frame size
          10, # class: connection
          70, # method: close-ok
          secret.bytesize, secret,
          reason.bytesize, reason,
          206 # frame end
        ].pack("C S> L> S> S> L>a* Ca* C")
      end

      def self.channel_open(id)
        [
          1, # type: method
          id, # channel id
          5, # frame size
          20, # class: channel
          10, # method: open
          0, # reserved1
          206 # frame end
        ].pack("C S> L> S> S> C C")
      end

      def self.channel_close(id, reason, code)
        frame_size = 2 + 2 + 2 + 1 + reason.bytesize + 2 + 2
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          20, # class: channel
          40, # method: close
          code,
          reason.bytesize, reason,
          0, # error class id
          0, # error method id
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* S> S> C")
      end

      def self.channel_close_ok(id)
        [
          1, # type: method
          id, # channel id
          4, # frame size
          20, # class: channel
          41, # method: close-ok
          206 # frame end
        ].pack("C S> L> S> S> C")
      end

      def self.exchange_declare(id, name, type, passive, durable, auto_delete, internal, arguments)
        no_wait = false
        bits = 0
        bits |= (1 << 0) if passive
        bits |= (1 << 1) if durable
        bits |= (1 << 2) if auto_delete
        bits |= (1 << 3) if internal
        bits |= (1 << 4) if no_wait
        tbl = Table.encode(arguments)
        frame_size = 2 + 2 + 2 + 1 + name.bytesize + 1 + type.bytesize + 1 + 4 + tbl.bytesize
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          40, # class: exchange
          10, # method: declare
          0, # reserved1
          name.bytesize, name,
          type.bytesize, type,
          bits,
          tbl.bytesize, tbl, # arguments
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* Ca* C L>a* C")
      end

      def self.exchange_delete(id, name, if_unused, no_wait)
        bits = 0
        bits |= (1 << 0) if if_unused
        bits |= (1 << 1) if no_wait
        frame_size = 2 + 2 + 2 + 1 + name.bytesize + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          40, # class: exchange
          20, # method: delete
          0, # reserved1
          name.bytesize, name,
          bits,
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* C C")
      end

      def self.exchange_bind(id, destination, source, binding_key, no_wait, arguments)
        tbl = Table.encode(arguments)
        frame_size = 2 + 2 + 2 + 1 + destination.bytesize + 1 + source.bytesize + 1 +
                     binding_key.bytesize + 1 + 4 + tbl.bytesize
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          40, # class: exchange
          30, # method: bind
          0, # reserved1
          destination.bytesize, destination,
          source.bytesize, source,
          binding_key.bytesize, binding_key,
          no_wait ? 1 : 0,
          tbl.bytesize, tbl, # arguments
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* Ca* Ca* C L>a* C")
      end

      def self.exchange_unbind(id, destination, source, binding_key, no_wait, arguments)
        tbl = Table.encode(arguments)
        frame_size = 2 + 2 + 2 + 1 + destination.bytesize + 1 + source.bytesize + 1 +
                     binding_key.bytesize + 1 + 4 + tbl.bytesize
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          40, # class: exchange
          40, # method: unbind
          0, # reserved1
          destination.bytesize, destination,
          source.bytesize, source,
          binding_key.bytesize, binding_key,
          no_wait ? 1 : 0,
          tbl.bytesize, tbl, # arguments
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* Ca* Ca* C L>a* C")
      end

      def self.queue_declare(id, name, passive, durable, exclusive, auto_delete, arguments)
        no_wait = false
        bits = 0
        bits |= (1 << 0) if passive
        bits |= (1 << 1) if durable
        bits |= (1 << 2) if exclusive
        bits |= (1 << 3) if auto_delete
        bits |= (1 << 4) if no_wait
        tbl = Table.encode(arguments)
        frame_size = 2 + 2 + 2 + 1 + name.bytesize + 1 + 4 + tbl.bytesize
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          50, # class: queue
          10, # method: declare
          0, # reserved1
          name.bytesize, name,
          bits,
          tbl.bytesize, tbl, # arguments
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* C L>a* C")
      end

      def self.queue_delete(id, name, if_unused, if_empty, no_wait)
        bits = 0
        bits |= (1 << 0) if if_unused
        bits |= (1 << 1) if if_empty
        bits |= (1 << 2) if no_wait
        frame_size = 2 + 2 + 2 + 1 + name.bytesize + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          50, # class: queue
          40, # method: declare
          0, # reserved1
          name.bytesize, name,
          bits,
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* C C")
      end

      def self.queue_bind(id, queue, exchange, binding_key, no_wait, arguments)
        tbl = Table.encode(arguments)
        frame_size = 2 + 2 + 2 + 1 + queue.bytesize + 1 + exchange.bytesize + 1 +
                     binding_key.bytesize + 1 + 4 + tbl.bytesize
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          50, # class: queue
          20, # method: bind
          0, # reserved1
          queue.bytesize, queue,
          exchange.bytesize, exchange,
          binding_key.bytesize, binding_key,
          no_wait ? 1 : 0,
          tbl.bytesize, tbl, # arguments
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* Ca* Ca* C L>a* C")
      end

      def self.queue_unbind(id, queue, exchange, binding_key, arguments)
        tbl = Table.encode(arguments)
        frame_size = 2 + 2 + 2 + 1 + queue.bytesize + 1 + exchange.bytesize + 1 +
                     binding_key.bytesize + 4 + tbl.bytesize
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          50, # class: queue
          50, # method: unbind
          0, # reserved1
          queue.bytesize, queue,
          exchange.bytesize, exchange,
          binding_key.bytesize, binding_key,
          tbl.bytesize, tbl, # arguments
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* Ca* Ca* L>a* C")
      end

      def self.queue_purge(id, queue, no_wait)
        frame_size = 2 + 2 + 2 + 1 + queue.bytesize + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          50, # class: queue
          30, # method: purge
          0, # reserved1
          queue.bytesize, queue,
          no_wait ? 1 : 0,
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* C C")
      end

      def self.basic_get(id, queue, no_ack)
        frame_size = 2 + 2 + 2 + 1 + queue.bytesize + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          70, # method: get
          0, # reserved1
          queue.bytesize, queue,
          no_ack ? 1 : 0,
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* C C")
      end

      def self.basic_publish(id, exchange, routing_key, mandatory)
        frame_size = 2 + 2 + 2 + 1 + exchange.bytesize + 1 + routing_key.bytesize + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          40, # method: publish
          0, # reserved1
          exchange.bytesize, exchange,
          routing_key.bytesize, routing_key,
          mandatory ? 1 : 0, # bits, mandatory/immediate
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* Ca* C C")
      end

      def self.header(id, body_size, properties)
        props = Properties.encode(properties)
        frame_size = 2 + 2 + 8 + props.bytesize
        [
          2, # type: header
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          0, # weight
          body_size,
          props, # properties
          206 # frame end
        ].pack("C S> L> S> S> Q> a* C")
      end

      def self.body(id, body_part)
        [
          3, # type: body
          id, # channel id
          body_part.bytesize, # frame size
          body_part,
          206 # frame end
        ].pack("C S> L> a* C")
      end

      def self.basic_consume(id, queue, tag, no_ack, exclusive, arguments)
        no_local = false
        no_wait = false
        bits = 0
        bits |= (1 << 0) if no_local
        bits |= (1 << 1) if no_ack
        bits |= (1 << 2) if exclusive
        bits |= (1 << 3) if no_wait
        tbl = Table.encode(arguments)
        frame_size = 2 + 2 + 2 + 1 + queue.bytesize + 1 + tag.bytesize + 1 + 4 + tbl.bytesize
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          20, # method: consume
          0, # reserved1
          queue.bytesize, queue,
          tag.bytesize, tag,
          bits, # bits
          tbl.bytesize, tbl, # arguments
          206 # frame end
        ].pack("C S> L> S> S> S> Ca* Ca* C L>a* C")
      end

      def self.basic_cancel(id, consumer_tag, no_wait: false)
        frame_size = 2 + 2 + 1 + consumer_tag.bytesize + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          30, # method: cancel
          consumer_tag.bytesize, consumer_tag,
          no_wait ? 1 : 0,
          206 # frame end
        ].pack("C S> L> S> S> Ca* C C")
      end

      def self.basic_cancel_ok(id, consumer_tag)
        frame_size = 2 + 2 + 1 + consumer_tag.bytesize + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          31, # method: cancel-ok
          consumer_tag.bytesize, consumer_tag,
          206 # frame end
        ].pack("C S> L> S> S> Ca* C")
      end

      def self.basic_ack(id, delivery_tag, multiple)
        frame_size = 2 + 2 + 8 + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          80, # method: ack
          delivery_tag,
          multiple ? 1 : 0,
          206 # frame end
        ].pack("C S> L> S> S> Q> C C")
      end

      def self.basic_nack(id, delivery_tag, multiple, requeue)
        bits = 0
        bits |= (1 << 0) if multiple
        bits |= (1 << 1) if requeue
        frame_size = 2 + 2 + 8 + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          120, # method: nack
          delivery_tag,
          bits,
          206 # frame end
        ].pack("C S> L> S> S> Q> C C")
      end

      def self.basic_reject(id, delivery_tag, requeue)
        frame_size = 2 + 2 + 8 + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          90, # method: reject
          delivery_tag,
          requeue ? 1 : 0,
          206 # frame end
        ].pack("C S> L> S> S> Q> C C")
      end

      def self.basic_qos(id, prefetch_size, prefetch_count, global)
        frame_size = 2 + 2 + 4 + 2 + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          10, # method: qos
          prefetch_size,
          prefetch_count,
          global ? 1 : 0,
          206 # frame end
        ].pack("C S> L> S> S> L> S> C C")
      end

      def self.basic_recover(id, requeue)
        frame_size = 2 + 2 + 1
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          60, # class: basic
          110, # method: recover
          requeue ? 1 : 0,
          206 # frame end
        ].pack("C S> L> S> S> C C")
      end

      def self.confirm_select(id, no_wait)
        [
          1, # type: method
          id, # channel id
          5, # frame size
          85, # class: confirm
          10, # method: select
          no_wait ? 1 : 0,
          206 # frame end
        ].pack("C S> L> S> S> C C")
      end

      def self.tx_select(id)
        frame_size = 2 + 2
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          90, # class: tx
          10, # method: select
          206 # frame end
        ].pack("C S> L> S> S> C")
      end

      def self.tx_commit(id)
        frame_size = 2 + 2
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          90, # class: tx
          20, # method: commit
          206 # frame end
        ].pack("C S> L> S> S> C")
      end

      def self.tx_rollback(id)
        frame_size = 2 + 2
        [
          1, # type: method
          id, # channel id
          frame_size, # frame size
          90, # class: tx
          30, # method: rollback
          206 # frame end
        ].pack("C S> L> S> S> C")
      end
    end
  end
end
