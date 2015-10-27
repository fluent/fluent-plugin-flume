require 'test/unit'
require 'fluent/test'
require 'lib/fluent/plugin/out_flume'

class FlumeOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    host 127.0.0.1
    port 35862
  ]

  def create_driver(conf=CONFIG, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::FlumeOutput, tag)do
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
    time = Time.parse("2011-12-21 13:14:15 UTC").to_i

    d = create_driver
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.emit({"k21"=>"v21", "k22"=>"v22"}, time)
    d.expect_format [d.tag, time, {"k11"=>"v11", "k12"=>"v12"}].to_msgpack
    d.expect_format [d.tag, time, {"k21"=>"v21", "k22"=>"v22"}].to_msgpack
    d.run

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'test.flumeplugin')
    assert_equal 'test.flumeplugin', d.tag

    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.emit({"k21"=>"v21", "k22"=>"v22"}, time)
    d.expect_format ['flumeplugin', time, {"k11"=>"v11", "k12"=>"v12"}].to_msgpack
    d.expect_format ['flumeplugin', time, {"k21"=>"v21", "k22"=>"v22"}].to_msgpack
    d.run

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'xxx.test.flumeplugin')
    assert_equal 'xxx.test.flumeplugin', d.tag
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.expect_format ['xxx.test.flumeplugin', time, {"k11"=>"v11", "k12"=>"v12"}].to_msgpack
    d.run

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'test')
    assert_equal 'test', d.tag
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.expect_format ['unknown', time, {"k11"=>"v11", "k12"=>"v12"}].to_msgpack
    d.run

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'test.flumeplugin')
    assert_equal 'test.flumeplugin', d.tag
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.emit({"k21"=>"v21", "k22"=>"v22"}, time)
    d.expect_format ['flumeplugin', time, {"k11"=>"v11", "k12"=>"v12"}].to_msgpack
    d.expect_format ['flumeplugin', time, {"k21"=>"v21", "k22"=>"v22"}].to_msgpack
    d.run
  end

  def test_write
    time = Time.parse("2011-12-21 13:14:15 UTC").to_i

    d = create_driver
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.emit({"k21"=>"v21", "k22"=>"v22"}, time)
    d.run
    emits = d.emits
    assert_equal [
        [d.tag, time, {"k11"=>"v11", "k12"=>"v12"}.to_json.to_s],
        [d.tag, time, {"k21"=>"v21", "k22"=>"v22"}.to_json.to_s],
      ], emits

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'test.flumeplugin')
    assert_equal 'test.flumeplugin', d.tag
    d.run do
      d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
      d.emit({"k21"=>"v21", "k22"=>"v22"}, time)
    end

    emits = d.emits
    assert_equal [
        ['flumeplugin', time, {"k11"=>"v11", "k12"=>"v12"}.to_json.to_s],
        ['flumeplugin', time, {"k21"=>"v21", "k22"=>"v22"}.to_json.to_s],
      ], emits

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'xxx.test.flumeplugin')
    assert_equal 'xxx.test.flumeplugin', d.tag
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.run
    emits = d.emits
    assert_equal [
        ['xxx.test.flumeplugin', time, {"k11"=>"v11", "k12"=>"v12"}.to_json.to_s],
      ], emits

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'test')
    assert_equal 'test', d.tag
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.run
    emits = d.emits
    assert_equal [
        [d.instance.default_category, time, {"k11"=>"v11", "k12"=>"v12"}.to_json.to_s],
      ], emits
  end
end
