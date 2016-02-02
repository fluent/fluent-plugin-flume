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
module Fluent

class FlumeInput < Input
  Plugin.register_input('flume', self)

  config_param :port,            :integer, :default => 56789
  config_param :bind,            :string,  :default => '0.0.0.0'
  config_param :server_type,     :string,  :default => 'simple'
  config_param :is_framed,       :bool,    :default => false
  config_param :body_size_limit, :size,    :default => 32 * 1024 * 1024
  config_param :tag_field,	 :string,  :default => nil
  config_param :default_tag,	 :string,  :default => 'category'
  config_param :add_prefix,      :string,  :default => nil
  config_param :msg_format,      :string,  :default => 'text'

  unless method_defined?(:log)
    define_method(:log) { $log }
  end

  def initialize
    require 'thrift'
    require 'fluent/plugin/old_thrift/flume_types'
    $:.unshift File.join(File.dirname(__FILE__), 'old_thrift')
    require 'flume_constants'
    require 'thrift_flume_event_server'
    super
  end

  def configure(conf)
    super
  end

  def start
    log.debug "listening flume on #{@bind}:#{@port}"

    handler = FluentFlumeHandler.new
    handler.tag_field = @tag_field
    handler.default_tag = @default_tag
    handler.add_prefix = @add_prefix
    handler.msg_format = @msg_format
    handler.log = log
    processor = ThriftFlumeEventServer::Processor.new handler

    @transport = Thrift::ServerSocket.new @bind, @port
    unless @is_framed
      transport_factory = Thrift::BufferedTransportFactory.new
    else
      raise ConfigError, "in_flume: unsupported is_framed '#{@is_framed}'"
    end

    unless ['text', 'json'].include? @msg_format
      raise 'Unknown format: msg_format=#{@msg_format}'
    end

    # 2011/09/29 Kazuki Ohta <kazuki.ohta@gmail.com>
    # This section is a workaround to set strict_read and strict_write option.
    # Ruby-Thrift 0.7 set them both 'true' in default, but Flume protocol set
    # them both 'false'.
    protocol_factory = Thrift::BinaryProtocolFactory.new
    protocol_factory.instance_eval {|obj|
      def get_protocol(trans) # override
        return Thrift::BinaryProtocol.new(trans,
                                          strict_read=false,
                                          strict_write=false)
      end
    }

    case @server_type
    when 'simple'
      @server = Thrift::SimpleServer.new processor, @transport, transport_factory, protocol_factory
    when 'threaded'
      @server = Thrift::ThreadedServer.new processor, @transport, transport_factory, protocol_factory
    when 'thread_pool'
      @server = Thrift::ThreadPoolServer.new processor, @transport, transport_factory, protocol_factory
    else
      raise ConfigError, "in_flume: unsupported server_type '#{@server_type}'"
    end
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @transport.close unless @transport.closed?
    #@thread.join
  end

  def run
    log.debug "starting server: #{@server}"
    @server.serve
  rescue
    log.error "unexpected error", :error=>$!.to_s
    log.error_backtrace
  end

  class FluentFlumeHandler
    attr_accessor :tag_field
    attr_accessor :default_tag
    attr_accessor :add_prefix
    attr_accessor :msg_format
    attr_accessor :log

    def append(evt)
      begin
        record = create_record(evt)
        if @tag_field
          tag = evt.fieldss[@tag_field] || @default_tag
          unless tag
            return # ignore
          end
        else
          tag = @default_tag
        end
        timestamp = evt.timestamp.to_i
        if @add_prefix
          Engine.emit(@add_prefix + '.' + tag, timestamp, record)
        else
          Engine.emit(tag, timestamp, record)
        end
      rescue => e
        log.error "unexpected error", :error=>$!.to_s
        log.error_backtrace
      end
    end

    def rawAppend(evt)
      log.error "rawAppend is not implemented yet: #{evt}"
    end

    def ackedAppend(evt)
      begin
        record = create_record(evt)
        if @tag_field
          tag = evt.fieldss[@tag_field] || @default_tag
          unless tag
            return # ignore
          end
        else
          tag = @default_tag
        end
        timestamp = evt.timestamp.to_i
        if @add_prefix
          Engine.emit(@add_prefix + '.' + tag, timestamp, record)
        else
          Engine.emit(tag, timestamp, record)
        end
        return EventStatus::ACK
      rescue => e
        log.error "unexpected error", :error=>$!.to_s
        log.error_backtrace
        return EventStatus::ERR
      end
    end

    def close()
    end

    private
    def create_record(evt)
      case @msg_format
      when 'text'
        return {'message'=>evt.body.force_encoding('UTF-8')}
      when 'json'
        js = JSON.parse(evt.body.force_encoding('UTF-8'))
        raise 'body must be a Hash, if json_body=true' unless js.is_a?(Hash)
        return js
      else
        raise 'Invalid format: #{@msg_format}'
      end
    end
  end
end

end
