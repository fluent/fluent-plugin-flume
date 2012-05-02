require 'test/unit'
require 'fluent/test'
require 'lib/fluent/plugin/out_flume'

class FlumeOutputTest < Test::Unit::TestCase
  CONFIG = %[
    host 127.0.0.1
    port 35862
  ]

  def create_driver(conf=CONFIG, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::FlumeOutput, tag).configure(conf)
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
    assert_equal [
        [d.tag, time, {"k11"=>"v11", "k12"=>"v12"}.to_json.to_s],
        [d.tag, time, {"k21"=>"v21", "k22"=>"v22"}.to_json.to_s],
      ], $handler.last
    $handler.last_clear

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'test.flumeplugin')
    assert_equal 'test.flumeplugin', d.tag
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.emit({"k21"=>"v21", "k22"=>"v22"}, time)
    d.run
    assert_equal [
        ['flumeplugin', time, {"k11"=>"v11", "k12"=>"v12"}.to_json.to_s],
        ['flumeplugin', time, {"k21"=>"v21", "k22"=>"v22"}.to_json.to_s],
      ], $handler.last
    $handler.last_clear

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'xxx.test.flumeplugin')
    assert_equal 'xxx.test.flumeplugin', d.tag
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.run
    assert_equal [
        ['xxx.test.flumeplugin', time, {"k11"=>"v11", "k12"=>"v12"}.to_json.to_s],
      ], $handler.last
    $handler.last_clear

    d = create_driver(CONFIG + %[
      remove_prefix test
    ], 'test')
    assert_equal 'test', d.tag
    d.emit({"k11"=>"v11", "k12"=>"v12"}, time)
    d.run
    assert_equal [
        [d.instance.default_category, time, {"k11"=>"v11", "k12"=>"v12"}.to_json.to_s],
      ], $handler.last
    $handler.last_clear
  end

  def setup
    Fluent::Test.setup
    Fluent::FlumeOutput.new
    $handler = TestFlumeServerHandler.new
    @dummy_server_thread = Thread.new do
      begin
        transport = Thrift::ServerSocket.new '127.0.0.1', 35862
        processor = ThriftFlumeEventServer::Processor.new $handler
        transport_factory = Thrift::BufferedTransportFactory.new
        protocol_factory = Thrift::BinaryProtocolFactory.new
        protocol_factory.instance_eval {|obj|
          def get_protocol(trans) # override
            Thrift::BinaryProtocol.new(trans, strict_read=false, strict_write=false)
          end
        }
        server = Thrift::SimpleServer.new processor, transport, transport_factory, protocol_factory
        server.serve
      ensure
        #transport.close unless transport.closed?
	transport.close if transport
      end
    end
    sleep 0.1 # boo...
  end

  def teardown
    @dummy_server_thread.kill
    @dummy_server_thread.join
  end

  class TestFlumeServerHandler
    attr :last
    def initialize
      @last = []
    end
    def append(evt)
      @last << [evt.fieldss['category'], evt.timestamp, evt.body]
    end
    def rawAppend(evt)
    end
    def ackedAppend(evt)
      EventStatus::OK
    end
    def close
    end
    def last_clear
      @last.clear
    end
  end

end
