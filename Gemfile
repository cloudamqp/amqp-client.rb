# frozen_string_literal: true

source "https://rubygems.org", cooldown: 7

# Specify your gem's dependencies in amqp-client.gemspec
gemspec

gem "benchmark"
gem "bunny"
# JRuby 10.1.1.0 bundles jruby-openssl 0.16.2, where SSLSocket#sysread flushes the
# shared netWriteData buffer without a lock, racing our writer threads against the
# read_loop thread and corrupting the TLS stream. 0.19.0 guards it with a writeLock.
gem "jruby-openssl", ">= 0.19.0", platforms: :jruby
gem "logger"
gem "minitest"
gem "minitest-mock"
gem "minitest-reporters"
gem "rake"
gem "rubocop"
gem "rubocop-minitest", require: false
gem "rubocop-rake", require: false
gem "stackprof", platforms: :ruby
gem "yard", require: false
