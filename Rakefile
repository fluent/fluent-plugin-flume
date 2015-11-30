require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake'
require 'rake/testtask'
require 'rake/clean'

task "thrift_gen" do
  system "mkdir -p tmp"
  system "thrift --gen rb -o tmp lib/fluent/plugin/thrift/flume.thrift"
  system "mv tmp/gen-rb/* lib/fluent/plugin/thrift/"
  system "rm -fR tmp"
end

Rake::TestTask.new(:test) do |t|
  t.libs << 'lib' << 'test'
  t.test_files = Dir['test/plugin/*.rb']
  t.ruby_opts = ['-rubygems'] if defined? Gem
  t.ruby_opts << '-I.'
end

#VERSION_FILE = "lib/fluent/version.rb"
#
#file VERSION_FILE => ["VERSION"] do |t|
#  version = File.read("VERSION").strip
#  File.open(VERSION_FILE, "w") {|f|
#    f.write <<EOF
#module Fluent
#
#VERSION = '#{version}'
#
#end
#EOF
#  }
#end
#
#task :default => [VERSION_FILE, :build]

task :default => [:build]
