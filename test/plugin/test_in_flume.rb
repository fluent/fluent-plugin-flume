require 'test/unit'
require 'fluent/test'
require 'fluent/plugin/in_flume'

class FlumeInputTest < Test::Unit::TestCase
  CONFIG = %[
    port 56790
    bind 127.0.0.1
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::FlumeInput).configure(conf)
  end

  def test_configure
    d = create_driver ''
    assert_equal 56789, d.instance.port
    assert_equal '0.0.0.0', d.instance.bind

    d = create_driver
    assert_equal 56790, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
  end

  def test_time
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "category", time, {"message"=>'aiueo'}
    d.expect_emit "category", time, {"message"=>'aiueo'}

    emits = [
      ['tag1', time, {"message"=>'aiueo'}],
      ['tag2', time, {"message"=>'aiueo'}]
    ]

    d.run do
      emits.each { |tag, time, record|
        send('tag', tag, time, record['message'])
        sleep 0.01
      }
    end

    d2 = create_driver(CONFIG + %[
      default_tag flume
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d2.expect_emit "flume", time, {"message"=>'aiueo'}
    d2.expect_emit "flume", time, {"message"=>'aiueo'}

    emits = [
      ['tag1', time, {"message"=>'aiueo'}],
      ['tag2', time, {"message"=>'aiueo'}]
    ]

    d2.run do
      emits.each { |tag, time, record|
        send('tag', tag, time, record['message'])
        sleep 0.01
      }
    end

    d3 = create_driver(CONFIG + %[
      tag_field flume
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d3.expect_emit "tag1", time, {"message"=>'aiueo'}
    d3.expect_emit "tag2", time, {"message"=>'aiueo'}

    emits = [
      ['tag1', time, {"message"=>'aiueo'}],
      ['tag2', time, {"message"=>'aiueo'}]
    ]

    d3.run do
      emits.each { |tag, time, record|
        send('flume', tag, time, record['message'])
        sleep 0.01
      }
    end

    d4 = create_driver(CONFIG + %[
      default_tag fluent
      tag_field flume
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d4.expect_emit "tag1", time, {"message"=>'aiueo'}
    d4.expect_emit "tag2", time, {"message"=>'aiueo'}

    emits = [
      ['tag1', time, {"message"=>'aiueo'}],
      ['tag2', time, {"message"=>'aiueo'}]
    ]

    d4.run do
      emits.each { |tag, time, record|
        send('flume', tag, time, record['message'])
        sleep 0.01
      }
    end
  end

  def test_add_prefix
    d = create_driver(CONFIG + %[
      add_prefix flume
      tag_field category
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "flume.tag1", time, {"message"=>'aiueo'}
    d.expect_emit "flume.tag2", time, {"message"=>'aiueo'}

    emits = [
      ['tag1', time, {"message"=>'aiueo'}],
      ['tag2', time, {"message"=>'aiueo'}],
    ]
    d.run do
      emits.each { |tag, time, record|
        send('category', tag, time, record['message'])
        sleep 0.01
      }
    end

    d2 = create_driver(CONFIG + %[
      add_prefix flume.input
      tag_field category
    ])

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d2.expect_emit "flume.input.tag3", time, {"message"=>'aiueo'}
    d2.expect_emit "flume.input.tag4", time, {"message"=>'aiueo'}

    emits = [
      ['tag3', time, {"message"=>'aiueo'}],
      ['tag4', time, {"message"=>'aiueo'}],
    ]
    d2.run do
      emits.each { |tag, time, record|
        send('category', tag, time, record['message'])
        sleep 0.01
      }
    end
  end

  def test_message_format_json
    d = create_driver(CONFIG + %[
      tag_field category
      message_format json
    ])
    assert_equal 'json', d.instance.message_format

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>1, "b"=>2}
    d.expect_emit "tag3", time, {"a"=>1, "b"=>2, "c"=>3}

    emits = [
      ['tag1', time, {"a"=>1}.to_json],
      ['tag2', time, {"a"=>1, "b"=>2}.to_json],
      ['tag3', time, {"a"=>1, "b"=>2, "c"=>3}.to_json],
    ]
    d.run do
      emits.each { |tag, time, message|
        send('category', tag, time, message)
        sleep 0.01
      }
    end
  end

  def setup
    omit("Latest flume thrift protocol does not have event server.")

    Fluent::FlumeInput.new
    Fluent::Test.setup
  end

  def send(tag_field, tag, time, msg)
    socket = Thrift::Socket.new '127.0.0.1', 56790
    #transport = Thrift::Socket.new '127.0.0.1', 56790
    transport = Thrift::BufferedTransport.new socket
    protocol = Thrift::BinaryProtocol.new transport
    client = ThriftFlumeEventServer::Client.new protocol
    transport.open
    raw_sock = socket.to_io
    raw_sock.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1

    entry = ThriftFlumeEvent.new(:body=>msg.to_s,
                                 :priority=>Priority::INFO,
                                 :timestamp=>time,
                                 :fieldss=>{tag_field=>tag})
    client.append entry
    transport.close
  end
end
