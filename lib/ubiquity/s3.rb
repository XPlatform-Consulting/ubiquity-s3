
require 'fog'
require 'logger'

module Ubiquity

  class S3

    def self.delete_object(args = { }); new(args).delete_object(args) end
    def self.upload(args = { }); new(args).upload(args) end

    attr_accessor :logger, :storage

    # Set the size of the file split
    DEFAULT_MULTIPART_CHUNK_SIZE = 5242880 # (5 * 1024 * 1024)


    def initialize(args = { })
      initialize_logger(args)
      initialize_storage(args)
    end

    def initialize_logger(args = { })
      @logger = args[:logger] ||= Logger.new(args[:log_to] ||= STDERR)
      logger.level = (log_level = args[:log_level]) ? log_level : Logger::DEBUG
      args[:logger] = logger
    end

    def initialize_storage(args = { })
      aws_key = args[:aws_key]
      raise ':aws_key is required to initialize a connection.' unless aws_key

      aws_secret = args[:aws_secret]
      raise ':aws_secret is required to initialize a connection.' unless aws_secret

      aws_region = args[:aws_region] || 'us-east-1'
      #raise ':aws_region is required to initialize a connection.' unless aws_region

      @default_bucket_name = args[:default_bucket_name] || args[:default_bucket_name]

      @storage = Fog::Storage.new({
        :provider => 'AWS',
        :aws_access_key_id => aws_key,
        :aws_secret_access_key => aws_secret,
        :region => aws_region
      })

      # Don't want to get caught with any timeout errors
      storage.sync_clock

    end

    def logger; @logger ||= Logger.new(STDERR) end

    def multipart_chunk_size
      @multipart_chunk_size ||= DEFAULT_MULTIPART_CHUNK_SIZE
    end

    def process_upload_options(args = { })
      options = { }
      headers = args[:request_headers] ||= { }
      headers.each { |key, value|
        case key
          when :cache_control
            options['Cache-Control'] = value
          when :content_disposition
            options['Content-Disposition'] = value
          when :content_encoding
            options['Content-Encoding'] = value
          when :content_length
            options['Content-Length'] = value
          when :content_md5
            options['Content-MD5'] = value
          when :content_type
            options['Content-Type'] = value
          when :expires
            options['Expires'] = value
          when :x_amz_acl
            options['x-amz-acl'] = value
          when :x_amz_storage_class, :storage_class
            options['x-amz-storage-class'] = value
          when :x_amz_server_side_encryption, :encryption
            options['x-amz-server-side-encryption'] = value
          when :x_amz_version_id, :version
            options['x-amz-version-id'] = value
          else
            options[key] = value
        end
      }
      options
    end

    def process_common_upload_arguments(args = { })
      bucket_name = args[:bucket_name] || args[:bucket] || @default_bucket_name

      path_of_file_to_upload = args[:path_of_file_to_upload]
      raise ArgumentError, ':path_of_file_to_upload is a required argument.' unless path_of_file_to_upload
      raise "File Not Found: #{path_of_file_to_upload}" unless File.exists?(path_of_file_to_upload)

      object_key = args[:object_key] || path_of_file_to_upload

      # Trim off any leading slashes otherwise we will get a directory name consisting of an empty string on S3
      object_key = object_key[1..-1] while object_key.start_with?('/') if object_key.respond_to?(:start_with?)
      raise ':object_key must be set and cannot be empty.' unless object_key and !object_key.empty?

      progress_callback = args[:progress_callback_method]

      options = process_upload_options(args)
      {
        :path_of_file_to_upload => path_of_file_to_upload,
        :bucket_name => bucket_name,
        :object_key => object_key,
        :options => options,
        :file_to_upload => File.open(path_of_file_to_upload),
        :progress_callback_method => progress_callback,
      }
    end

    # Gets just the header of the file which includes size, url, etc.
    #
    # @param [String] bucket_name
    # @param [String] object_key
    # @return (see #Fog::Storage::File)
    def get_file_head(bucket_name, object_key)
      directory = storage.directories.get( bucket_name )
      directory.files.head( object_key )
    end

    # @param [Hash] args
    # @option args [String] :bucket_name The name of the bucket that the file to delete is located in.
    # @option args [String] :object_key The name of the file to delete.
    def delete_object(args = { })
      bucket_name = args[:bucket_name] || args[:bucket] || @default_bucket_name
      raise ArgumentError, ':bucket_name must be set and cannot be empty.' unless bucket_name.respond_to?(:empty?) and !bucket_name.empty?

      object_key = args[:object_key]
      object_key = object_key[1..-1] while object_key.start_with?('/') if object_key.respond_to?(:start_with?)
      raise ArgumentError, ':object_key must be set and cannot be empty.' unless object_key.respond_to?(:empty?) and !object_key.empty?

      bucket = storage.directories.new( :key => bucket_name )
      file = bucket.files.new( :key => object_key )
      file.destroy
    end

    # @param [Hash] args
    # @option args [String] :path_of_file_to_upload
    # @option args []
    def upload(args = { })
      if args[:use_multipart_upload]
        response = upload_multipart(args)
      else
        upload_args = process_common_upload_arguments(args)

        file_size = upload_args[:file_to_upload].size
        file_upload_start = Time.now
        response = storage.put_object(upload_args[:bucket_name], upload_args[:object_key], upload_args[:file_to_upload], upload_args[:options])
        file_upload_end = Time.now
        file_upload_took  = file_upload_end - file_upload_start
        logger.debug { "Uploaded #{file_size} bytes in #{file_upload_took.round(2)} seconds. #{(file_size/file_upload_took).round(0)} Bps" }
      end
      return response
    end

    def upload_multipart(args = { })
      upload_args = process_common_upload_arguments(args)

      progress_callback = args[:progress_callback_method]
      file = upload_args[:file_to_upload]
      _multipart_chunk_size = args[:multipart_chunk_size] || multipart_chunk_size

      response = storage.initiate_multipart_upload(upload_args[:bucket], upload_args[:object_key], upload_args[:options])
      upload_id = response.body['UploadId']

      part_tags = []

      file_size = file.size
      total_parts = (file_size.to_f / _multipart_chunk_size.to_f).ceil

      file.rewind if file.respond_to?(:rewind)

      part_counter = 0
      file_upload_start = file_upload_end = nil
      while (chunk = file.read(_multipart_chunk_size)) do
        part_counter += 1
        logger.debug { "Uploading Part #{part_counter} of #{total_parts}" }

        progress_callback.call(nil, "Uploading Part #{part_counter} of #{total_parts}", ((part_counter/total_parts) * 100)) if progress_callback

        md5 = Base64.encode64(Digest::MD5.digest(chunk)).strip
        part_upload_start = Time.now
        part_upload = storage.upload_part(upload_args[:bucket], upload_args[:object_key], upload_id, part_tags.size + 1, chunk, 'Content-MD5' => md5 )
        part_upload_end = Time.now
        part_upload_took = part_upload_end - part_upload_start
        logger.debug { "#{_multipart_chunk_size} took #{part_upload_took.round(2)} seconds. #{(_multipart_chunk_size / part_upload_took).round(0)} Bps." }
        part_tags << part_upload.headers['ETag']
        file_upload_start ||= part_upload_start
        file_upload_end = part_upload_end
      end
      file_upload_took = file_upload_end - file_upload_start
      logger.debug { "Uploaded #{file_size} bytes in #{file_upload_took.round(2)} seconds. #{(file_size/file_upload_took).round(0)} Bps" }
    rescue
      storage.abort_multipart_upload(upload_args[:bucket], upload_args[:object_key], upload_id) if upload_id
      raise
    else
      return storage.complete_multipart_upload(upload_args[:bucket], upload_args[:object_key], upload_id, part_tags)
    end

    def upload_multipart_threaded(args = { })

    end

  end

end