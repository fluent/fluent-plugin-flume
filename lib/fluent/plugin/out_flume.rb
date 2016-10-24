#
# Fluent
#
# Copyright (C) 2012 - 2013 Muga Nishizawa
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/output'

module Fluent

class FlumeOutput < BufferedOutput
  Fluent::Plugin.register_output('flume', self)

  config_param :host,      :string,  :default => 'localhost'
  config_param :port,      :integer, :default => 35863
  config_param :timeout,   :integer, :default => 30
  config_param :remove_prefix,    :string, :default => nil
  config_param :default_category, :string, :default => 'unknown'
  desc "The format of the thrift body. (default: json)"
  config_param :format, :string, default: 'json'
  config_param :trim_nl, :bool, default: true

  unless method_defined?(:log)
    define_method(:log) { $log }
  end

  def initialize
    require 'thrift'
    $:.unshift File.join(File.dirname(__FILE__), 'thrift')
    require 'flume_types'
    require 'flume_constants'
    require 'thrift_source_protocol'
    super
  end

  def configure(conf)
    # override default buffer_chunk_limit
    conf['buffer_chunk_limit'] ||= '1m'

    super

    @formatter = Plugin.new_formatter(@format)
    @formatter.configure(conf)
  end

  def start
    super

    if @remove_prefix
      @removed_prefix_string = @remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    if @remove_prefix and
        ((tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length) or tag == @remove_prefix)
      tag = (tag[@removed_length..-1] || @default_category)
    end
    fr = @formatter.format(tag, time, record)
    fr.chomp! if @trim_nl
    [tag, time, fr].to_msgpack
  end

  def write(chunk)
    socket = Thrift::Socket.new @host, @port, @timeout
    transport = Thrift::FramedTransport.new socket
    #protocol = Thrift::BinaryProtocol.new transport, false, false
    protocol = Thrift::CompactProtocol.new transport
    client = ThriftSourceProtocol::Client.new protocol

    count = 0
    header = {}
    transport.open
    log.debug "thrift client opened: #{client}"
    begin
      chunk.msgpack_each { |tag, time, record|
        header['timestamp'.freeze] = time.to_s
        header['tag'.freeze] = tag
        entry = ThriftFlumeEvent.new(:body    => record.force_encoding('UTF-8'),
                                     :headers => header)
        client.append entry
        count += 1
      }
      log.debug "Writing #{count} entries to flume"
    ensure
      log.debug "thrift client closing: #{client}"
      transport.close
    end
  end
end

end
