require 'aws/s3'
require 'logging'
require 'thread'

module Timber
  class S3Appender < Logging::Appender
    include Logging::Appenders::Buffering

    DEFAULT_AGE = 500
    DEFAULT_SIZE = 16 * 1024
    DEFAULT_EVENT_BUFFER = 50

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
      super(name, opts)
      @bucket_name    = bucket_name
      @prefix         = object_prefix
  
      @age_limit      = opts.delete(:age)           || DEFAULT_AGE
      @size_limit     = opts.delete(:size)          || DEFAULT_SIZE
      @buffer_limit   = opts.delete(:event_buffer)  || DEFAULT_EVENT_BUFFER
      @sending_queue  = Queue.new

      configure_buffering(opts.merge(:auto_flushing => @buffer_limit))
      start_new_log
      start_sending_thread 
    end

    def flush
      buffer_to_send = buffer.dup
      buffer.clear

      send_buffer(current_object_name, buffer_to_send) unless buffer_to_send.empty?

      start_new_log
      self
    end

    def close
      flush
      @sending_queue.push [-1, -1]
      @sending_thread.join
    end

    def write(event)
      super
      @buffered_amount += event.size

      if @buffered_amount > @size_limit
        flush
      end
    end

    private

    def current_object_name
      "#{@prefix}-#{(@log_begins.to_f * 100_000).to_i}"
    end

    def send_buffer(key, buffer_to_send)
      @sending_queue.push [key, buffer_to_send]
    end

    def start_new_log
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

    def start_sending_thread
      @sending_thread = Thread.new do
        loop do
          key, buffer_to_send = @sending_queue.pop
          break if key == -1 && buffer_to_send == -1

          AWS::S3::S3Object.store(key, buffer_to_send.join, @bucket_name)
        end
      end
    end
  end
end

