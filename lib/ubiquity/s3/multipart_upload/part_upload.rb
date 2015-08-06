require 'base64'
require 'digest/md5'
require 'stringio'

module Ubiquity

  class S3

    class MultipartUpload

      class PartUpload

        attr_reader :bucket_name, :object_key, :upload_id, :index, :part_number, :data, :md5, :response, :time_started, :time_ended

        def initialize(args = { })
          @s3 = args[:s3]
          @bucket_name = args[:bucket_name]
          @object_key = args[:object_key]
          @upload_id = args[:upload_id]
          @index = args[:index]
          @part_number = index + 1
          @data = args[:data]
          @md5 = args[:md5] ||= generate_md5
          @response = nil
        end

        def bytes_per_second
          @size / @time_elapsed
        end

        def do_upload
          @time_started = Time.now
          @response = @s3.upload_part(bucket_name, object_key, upload_id, part_number, data, { 'Content-MD5' => md5 })
          @time_elapsed = ((@time_ended = Time.now) - @time_started)
          response
        end

        def etag
          return unless @response
          response.headers['ETag']
        end

        def size
          @size ||= data.length
        end

        def status_as_hash
          {
            :size => size,
            :time_started => time_started,
            :time_elapsed => time_elapsed,
            :bytes_per_second => bytes_per_second,
          }
        end

        def success?
          !!etag
        end
        alias :successful? :success?

        def time_elapsed
          @time_elapsed || (Time.now - @time_started)
        end

        def generate_md5(data = @data)
          case data
            when String
              part = StringIO.new(data)
            when IO, File
              part = data
            else
              raise "Unknown Part Data Type: #{data.class.name}"
          end
          md5 = Digest::MD5.digest(part.read)
          Base64.encode64(md5).strip
        end

        def inspect
          # Have to do to_sym on the instance variable because it is a string in Ruby 1.8.7 but a symbol in 1.9 and up
          "<#{self.class.name}:#{self.object_id}" + instance_variables.sort.map { |i| " #{i.to_s}=#{[:@s3, :@data].include?(i.to_sym) ? '...' : instance_variable_get(i)}" }.join('') + '>'
        end

      end

    end

  end

end