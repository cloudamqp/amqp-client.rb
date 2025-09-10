# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.description = "Run all but TLS tests"
  t.options = "--exclude=/_tls$/"
  t.pattern = "test/**/*_test.rb"
end

namespace :test do
  Rake::TestTask.new(:all) do |t|
    t.description = "Run all tests"
    t.pattern = "test/**/*_test.rb"
  end
end

require "rubocop/rake_task"

RuboCop::RakeTask.new

require "yard"

YARD::Rake::YardocTask.new

# Release helper methods
def current_version
  version_file = "lib/amqp/client/version.rb"
  content = File.read(version_file)
  content.match(/VERSION = "(.+)"/)[1]
end

def bump_version(version_type)
  unless %w[major minor patch].include?(version_type)
    puts "Invalid version type. Use: major, minor, or patch"
    exit 1
  end

  version_file = "lib/amqp/client/version.rb"
  content = File.read(version_file)

  current_version = content.match(/VERSION = "(.+)"/)[1]
  major, minor, patch = current_version.split(".").map(&:to_i)

  case version_type
  when "major"
    major += 1
    minor = 0
    patch = 0
  when "minor"
    minor += 1
    patch = 0
  when "patch"
    patch += 1
  end

  new_version = "#{major}.#{minor}.#{patch}"
  new_content = content.gsub(/VERSION = ".+"/, %(VERSION = "#{new_version}"))

  File.write(version_file, new_content)
  puts "Bumped version from #{current_version} to #{new_version}"
end

def update_changelog
  version = current_version
  date = Time.now.strftime("%Y-%m-%d")

  changelog = File.read("CHANGELOG.md")

  if changelog.include?("## [#{version}]")
    puts "Version #{version} already exists in CHANGELOG.md"
  else
    updated_changelog = changelog.sub(
      "## [Unreleased]",
      "## [Unreleased]\n\n## [#{version}] - #{date}"
    )

    File.write("CHANGELOG.md", updated_changelog)
    puts "Updated CHANGELOG.md with version #{version}"
  end
end

def create_git_tag
  version = current_version

  system("git add .")
  system("git commit -m 'Release #{version}'")

  # Check if tag already exists and remove it if it does
  if system("git tag -l v#{version} | grep -q v#{version}")
    puts "Tag v#{version} already exists, removing it..."
    system("git tag -d v#{version}")
  end

  system("git tag v#{version}")

  puts "Created git tag v#{version}"
end

def push_gem_to_rubygems
  version = current_version

  # Look for gem file in both current directory and pkg directory
  gem_file = "amqp-client-#{version}.gem"
  pkg_gem_file = "pkg/amqp-client-#{version}.gem"

  if File.exist?(pkg_gem_file)
    system("gem push #{pkg_gem_file}")
    puts "Pushed #{pkg_gem_file} to RubyGems"
  elsif File.exist?(gem_file)
    system("gem push #{gem_file}")
    puts "Pushed #{gem_file} to RubyGems"
  else
    puts "Gem file #{gem_file} not found in current directory or pkg/. Make sure to build first."
    exit 1
  end
end

def full_release_process(version_type)
  puts "Starting release process..."

  # Ensure working directory is clean
  unless system("git diff --quiet && git diff --cached --quiet")
    puts "Working directory is not clean. Please commit or stash changes first."
    exit 1
  end

  # Bump version
  Rake::Task["release:bump"].invoke(version_type)
  Rake::Task["release:bump"].reenable

  # Update changelog
  Rake::Task["release:changelog"].invoke
  Rake::Task["release:changelog"].reenable

  # Create tag and push
  Rake::Task["release:tag"].invoke
  Rake::Task["release:tag"].reenable

  # Build and push gem
  Rake::Task["release:push"].invoke
  Rake::Task["release:push"].reenable

  # Push to git
  system("git push origin")
  system("git push origin --tags")

  version = current_version
  puts "Successfully released version #{version}!"
end

namespace :release do
  desc "Bump version (usage: rake release:bump[major|minor|patch])"
  task :bump, [:type] do |_t, args|
    bump_version(args[:type] || "patch")
  end

  desc "Update changelog with current version"
  task :changelog do
    update_changelog
  end

  desc "Create git tag for current version"
  task :tag do
    create_git_tag
  end

  desc "Build and push gem to RubyGems"
  task push: :build do
    push_gem_to_rubygems
  end

  desc "Full release process (bump version, update changelog, tag, build and push)"
  task :full, [:type] => %i[test rubocop] do |_t, args|
    full_release_process(args[:type] || "patch")
  end
end

task default: [:test, *(:rubocop if ENV["CI"] != "true")]
