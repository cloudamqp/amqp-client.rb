# frozen_string_literal: true

source "https://rubygems.org", cooldown: 7

# Specify your gem's dependencies in amqp-client.gemspec
gemspec

# JRuby 10.1.1.0 bundles jruby-openssl 0.16.2, where SSLSocket#sysread flushes the
# shared netWriteData buffer without a lock, racing our writer threads against the
# read_loop thread and corrupting the TLS stream. 0.19.0 guards it with a writeLock.
gem "jruby-openssl", ">= 0.19.0", platforms: :jruby
gem "logger"
gem "minitest"
gem "minitest-mock"
gem "minitest-reporters"
gem "rake"
gem "yard", require: false

# The gem uses json and openssl without depending on them. Testing every
# version isn't reasonable, so CI tests with the latest and with the ones Ruby
# ships, by leaving out this group and the groups that depend on them.
group :default_gems do
  gem "json"
  gem "openssl"
end

group :benchmark do
  gem "benchmark"
  gem "bunny"
  gem "stackprof", platforms: :ruby
end

group :rubocop do
  gem "rubocop"
  gem "rubocop-minitest", require: false
  gem "rubocop-rake", require: false
end
