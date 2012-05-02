require 'test/unit'
require 'pathname'

$LOAD_PATH.unshift("./lib")
$LOAD_PATH.unshift("./lib/fluent/plugin/thrift")

require 'thrift'
require 'flume_constants'
require 'flume_types'
require 'thrift_flume_event_server'

class FluentFlumeHandler
  def append(evt)
    puts "call append(evt: #{evt})"
    puts "evt: #{evt}"
    puts "  timestamp: #{evt.timestamp.to_i / 1000}"
    puts "  body:      #{evt.body}"
    puts "  fieldss:   #{evt.fieldss}"
  end

  def rawAppend(evt)
    puts "call rawAppend(evt: #{evt})"
  end

  def ackedAppend(evt)
    puts "call ackedAppend(evt: #{evt})"
    EventStatus::OK
  end

  def close
    puts "call close()"
  end
end

handler = FluentFlumeHandler.new
processor = ThriftFlumeEventServer::Processor.new handler

transport = Thrift::ServerSocket.new 'localhost', 56789
#transport_factory = Thrift::FramedTransportFactory.new
transport_factory = Thrift::BufferedTransportFactory.new

protocol_factory = Thrift::BinaryProtocolFactory.new
protocol_factory.instance_eval {|obj|
  def get_protocol(trans) # override
    return Thrift::BinaryProtocol.new(trans,
                                      strict_read=false,
                                      strict_write=false)
  end
}

server = Thrift::SimpleServer.new processor, transport, transport_factory, protocol_factory
# ok # server = Thrift::ThreadedServer.new processor, transport, transport_factory, protocol_factory
# ok # server = Thrift::ThreadPoolServer.new processor, transport, transport_factory, protocol_factory
# ng # server = Thrift::NonblockingServer.new processor, transport, transport_factory, protocol_factory

puts "server start!"
server.serve
