require 'aws-s3'
require 'logging'
require 'thread'

module Timber
  class S3Appender < Logging::Appender
    include Buffering

    # call-seq:
    #   S3Appender.new( name, bucket, object_prefix, options )
    #
    # Creates a new S3 Appender. The _name_ is the unique Appender name used 
    # to retrieve this appender from the Appender hash.  _bucket_ is the S3
    # bucket to store the log files in. _object_prefix_ is prefixed to all
    # S3 Objects the logger creates. You can use the options to specify
    # conditions which will cause the log to be pushed to S3.  By default the
    # log is pushed every 16KB or every 5 minutes.
    #
    # The following options are available:
    #
    #   [:age]            The maximum amount of time (in seconds) before a log is pushed to S3. (Default: 300)
    #   [:size]           The maximum size before a file is pushed to S3. (Default: 5)
    #   [:event_buffer]   The maximum number of events before the file is pushed to S3. (Default: 0)
    #
    # Setting any of these options to 0 will cause the log never be pushed 
    # under that condition. It will push when the first condition is surpassed.
    #
    def initialize(name, bucket_name, object_prefix, opts = {})
      @bucket_name  = bucket_name
      @prefix       = prefix

      @age_limit      = opts.delete(:age) || DEFAULT_AGE
      @size_limit     = opts.delete(:age) || DEFAULT_SIZE
      @buffer_limit   = opts.delete(:age) || DEFAULT_EVENT_BUFFER

      start_new_log
    end

    def flush
      buffer_to_send = buffer
      buffer.clear

      send_buffer(current_object_name, buffer_to_send) unless buffer_to_send.empty?

      start_new_log
      self
    end

    def close
      flush
    end

    def write(event)
      super
      @buffer_size += event.size

      if @buffer_size > @buffer_limit
        flush
      end
    end

    private

    def current_object_name
      "#{@prefix}-#{@log_begins.to_i}"
    end

    def send_buffer(buffer_to_send)
      Thread.start {
        #AWS::S3::S3Object.store
      }
    end

    def start_new_log
      @buffer       = []
      @buffer_size  = 0
      @log_begins   = Time.new
      set_timeout 
    end

    def set_timeout
      # clear out timeout
      @timeout_thread.kill if @timeout_thread
      # start new timeout
      @timeout_thread = Thread.start {
        sleep @age_limit
        flush
      } unless @age_limit == 0
    end
  end
end
