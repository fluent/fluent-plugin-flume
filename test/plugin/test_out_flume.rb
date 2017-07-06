require 'test/unit'
require 'fluent/test'
require 'fluent/test/helpers'
require 'fluent/test/driver/output'
require 'lib/fluent/plugin/out_flume'

class FlumeOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    host 127.0.0.1
    port 35862
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::FlumeOutput) do
      def write(chunk)
        chunk.read
      end
    end.configure(conf)
  end

  def test_configure
    d = create_driver('')

    assert_equal 'localhost', d.instance.host
    assert_equal 35863, d.instance.port
    assert_equal 30, d.instance.timeout
    assert_equal 'unknown', d.instance.default_category
    assert_nil d.instance.remove_prefix

    d = create_driver

    assert_equal '127.0.0.1', d.instance.host
    assert_equal 35862, d.instance.port
  end

  def test_format
    time = event_time("2011-12-21 13:14:15 UTC")

    d = create_driver
    d.run(default_tag: 'test') do
      d.feed(time, {"k11"=>"v11", "k12"=>"v12"})
      d.feed(time, {"k21"=>"v21", "k22"=>"v22"})
    end
    assert_equal [time, {"k11"=>"v11", "k12"=>"v12"}.to_json].to_msgpack, d.formatted[0]
    assert_equal [time, {"k21"=>"v21", "k22"=>"v22"}.to_json].to_msgpack, d.formatted[1]

    d = create_driver(CONFIG + %[
      remove_prefix test
    ])

    d.run(default_tag: 'test.flumeplugin') do
      d.feed(time, {"k11"=>"v11", "k12"=>"v12"})
      d.feed(time, {"k21"=>"v21", "k22"=>"v22"})
    end
    assert_equal [time, {"k11"=>"v11", "k12"=>"v12"}.to_json].to_msgpack, d.formatted[0]
    assert_equal [time, {"k21"=>"v21", "k22"=>"v22"}.to_json].to_msgpack, d.formatted[1]

    d = create_driver(CONFIG + %[
      remove_prefix test
    ])
    d.run(default_tag: 'xxx.test.flumeplugin') do
      d.feed(time, {"k11"=>"v11", "k12"=>"v12"})
    end

    d = create_driver(CONFIG + %[
      remove_prefix test
    ])
    d.run(default_tag: 'test') do
      d.feed(time, {"k11"=>"v11", "k12"=>"v12"})
    end
    assert_equal [time, {"k11"=>"v11", "k12"=>"v12"}.to_json].to_msgpack, d.formatted[0]

    d = create_driver(CONFIG + %[
      remove_prefix test
    ])
    d.run(default_tag: 'test.flumeplugin') do
      d.feed(time, {"k11"=>"v11", "k12"=>"v12"})
      d.feed(time, {"k21"=>"v21", "k22"=>"v22"})
    end
    assert_equal [time, {"k11"=>"v11", "k12"=>"v12"}.to_json].to_msgpack, d.formatted[0]
    assert_equal [time, {"k21"=>"v21", "k22"=>"v22"}.to_json].to_msgpack, d.formatted[1]
  end

  def test_write
    time = event_time("2011-12-21 13:14:15 UTC")

    d = create_driver
    d.run(default_tag: 'test') do
      d.feed(time, {"k11"=>"v11", "k12"=>"v12"})
      d.feed(time, {"k21"=>"v21", "k22"=>"v22"})
    end

    d = create_driver(CONFIG + %[
      remove_prefix test
    ])
    d.end_if do
      d.events.size >= 0
    end
    d.run(default_tag: 'test.flumeplugin')

    d = create_driver(CONFIG + %[
      remove_prefix test
    ])
    d.run(default_tag: 'xxx.test.flumeplugin') do
      d.feed(time, {"k11"=>"v11", "k12"=>"v12"})
    end

    d = create_driver(CONFIG + %[
      remove_prefix test
    ])
    d.run(default_tag: 'test') do
      d.feed(time, {"k11"=>"v11", "k12"=>"v12"})
    end
  end
end
