require 'test/unit'
require 'pathname'

$LOAD_PATH.unshift("./lib")
$LOAD_PATH.unshift("./lib/fluent/plugin/thrift")

require 'thrift'
require 'flume_constants'
require 'flume_types'
require 'thrift_flume_event_server'

#socket = Thrift::Socket.new 'localhost', 56789, 3000
#transport = Thrift::Socket.new 'localhost', 56789, 3000
#transport = Thrift::FramedTransport.new(Thrift::Socket.new('localhost', 56789, 3000))
transport = Thrift::BufferedTransport.new(Thrift::Socket.new('localhost', 56789, 3000))
#protocol = Thrift::BinaryProtocol.new transport, false, false
protocol = Thrift::BinaryProtocol.new(transport)
client = ThriftFlumeEventServer::Client.new(protocol)

transport.open

[1, 2].each { |elm|
message = 'muganishizawa'
priority = Priority::INFO
entry = ThriftFlumeEvent.new(:body => message, :priority => priority, :timestamp => (Time.now.to_i * 1000))
#entry = ThriftFlumeEvent.new
client.append entry
}

transport.close