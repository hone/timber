module Timber
  class LogFile
    attr_accessor :size, :uuid, :objects, :start_time, :end_time, :metadata

    def self.find(bucket, prefix = '')
      objects = AWS::S3::Bucket.objects(bucket, :prefix => prefix)

      logs = {}
      objects.each do |o|
        uuid = extract_uuid(o.key)

        logs[uuid] ||= []
        logs[uuid]  << o
      end

      logs.collect do |_, objs|
        new(objs)
      end
    end

    def initialize(objects)
      @objects                = objects
      @current_object_idx     = 0
      @current_object_offset  = 0

      @position = 0

      @size = @objects.inject(0) { |sum,o| sum += o.size }
      
      extract_info
    end

    def extract_info
      first_object  = @objects.first

      @uuid       = LogFile.extract_uuid(first_object.key)
      @start_time = LogFile.extract_timestamp(first_object.key)
      @end_time   = LogFile.extract_timestamp(@objects.last.key)
      @metadata   = first_object.metadata.inject({}) { |hash, kv| hash[k.first.gsub("x-amz-meta-","")] = k.last; hash }
    end

    def seek(to, whence = IO::SEEK_SET)
    end

    def read(length = nil)
      if length
        data = read_bytes(length)
        @position += length 
      else
        return read(size - @position)
      end
      data
    end

    def aggregate!(bs = 16 * (2**20))
    end

    def refresh!
=begin
      marker = "#{@objects.last.key}"
      prefix = marker.gsub(/\/.*$/,'')
      new_objects = AWS::S3::Bucket.objects(bucket, :prefix => prefix, :marker => marker)
=end
    end

    private
    def self.extract_uuid(key)
      key.split("/")[1]
    end

    def self.extract_timestamp(key)
      Time.at(key.split("/")[2].to_i / 100_000)
    end

    def read_bytes(bytes)
      read_slice(@position, bytes)
    end

    def current_object
      @objects[@current_object_idx]
    end

    # Reads at most _bytes_ from current object
    def read_from_current_object(bytes)
      bytes_left = current_object.size - @current_object_offset

      if bytes_left > bytes
        data = current_object.value.slice(@current_object_offset, bytes)
        @current_object_offset += bytes
      elsif bytes_left <= bytes
        data = current_object.value.slice(@current_object_offset, bytes_left)
        @current_object_idx   += 1
        @current_object_offset = 0
      end
      return data
    end

    def read_slice(start, n)
      #TODO sanity check
      to_consume    = n
      buffer        = ""
      while to_consume > 0
        data          = read_from_current_object(to_consume)
        to_consume   -= data.size
        buffer       += data
      end
      return buffer
    end
  end
end
