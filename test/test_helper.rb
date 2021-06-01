# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "amqp/client"

require "minitest/autorun"

Thread.abort_on_exception = true
