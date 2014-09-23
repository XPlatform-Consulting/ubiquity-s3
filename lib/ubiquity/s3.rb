require 'logger'

require 'fog'

require 'ubiquity/s3/multipart_upload'

module Ubiquity

  class S3

    def self.delete_object(args = { }); new(args).delete_object(args) end
    def self.upload(args = { }); new(args).upload(args) end

    attr_accessor :logger, :storage

    attr_accessor :default_bucket_name,
                  :default_multipart_upload_allowed,
                  :default_multipart_upload_chunk_size,
                  :default_multipart_upload_threshold


    DEFAULT_AWS_REGION = 'us-east-1'

    # Set the size of the file split
    DEFAULT_MULTIPART_UPLOAD_CHUNK_SIZE = 5242880 # (5 * 1024 * 1024)
    DEFAULT_MULTIPART_UPLOAD_ALLOWED = true
    DEFAULT_MULTIPART_UPLOAD_THRESHOLD = 60000000 # 60MB
    DEFAULT_MULTIPART_UPLOAD_THREAD_LIMIT = 10

    MULTIPART_UPLOAD_PART_LIMIT = 10000


    def initialize(args = { })
      initialize_logger(args)
      initialize_storage(args)
    end

    # @param [Hash] args
    # @option args [Logger]     :logger A logger to be used
    # @option args [IO, String] :log_to An IO device or file to log to
    # @option args [Integer]    :log_level (Logger::DEBUG) The logging level to be set to the logger
    def initialize_logger(args = { })
      @logger = args[:logger] ||= Logger.new(args[:log_to] ||= STDERR)
      logger.level = (log_level = args[:log_level]) ? log_level : Logger::DEBUG
      args[:logger] = logger
    end

    # @param [Hash] args
    # @option args [String] :aws_key The Amazon Web Services (AWS) access key
    # @option args [String] :aws_secret The AWS secret key
    # @option args [String] :aws_region ('us-east-1') The AWS Region
    # @option args [String] :default_bucket_name The default bucket name to use for operations that support it
    def initialize_storage(args = { })
      aws_key = args[:aws_key]
      raise ':aws_key is required to initialize a connection.' unless aws_key

      aws_secret = args[:aws_secret]
      raise ':aws_secret is required to initialize a connection.' unless aws_secret

      aws_region = args[:aws_region] || DEFAULT_AWS_REGION
      #raise ':aws_region is required to initialize a connection.' unless aws_region

      @default_bucket_name = args[:default_bucket_name] || args[:default_bucket]
      @default_multipart_upload_allowed = args.fetch(:default_multipart_upload_allowed, DEFAULT_MULTIPART_UPLOAD_ALLOWED)
      @default_multipart_upload_chunksize = args[:default_multipart_upload_chunk_size] || DEFAULT_MULTIPART_UPLOAD_CHUNK_SIZE
      @default_multipart_upload_threshold = args[:default_multipart_upload_threshold] || DEFAULT_MULTIPART_UPLOAD_THRESHOLD

      args_out = {
        :provider => 'AWS',
        :aws_access_key_id => aws_key,
        :aws_secret_access_key => aws_secret,
        :region => aws_region
      }
      #:endpoint, :region, :host, :port, :scheme, :persistent, :use_iam_profile, :aws_session_token, :aws_credentials_expire_at, :path_style
      Fog::Storage::AWS.recognized.each { |k| args_out[k] = args[k] if args.has_key?(k) }

      @storage = Fog::Storage.new(args_out)

      # Don't want to get caught with any timeout errors
      storage.sync_clock

    end

    def logger; @logger ||= Logger.new(STDERR) end

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

      progress_callback_method = args[:progress_callback_method]

      overwrite_existing_file = args.fetch(:overwrite_existing_file, false)
      overwrite_existing_file_only_if_size_mismatch = args.fetch(:overwrite_existing_file_if_size_mismatch, false)
      overwrite_existing_file ||= overwrite_existing_file_only_if_size_mismatch

      options = process_upload_options(args)
      {
        :path_of_file_to_upload => path_of_file_to_upload,
        :bucket_name => bucket_name,
        :object_key => object_key,
        :options => options,
        :file_to_upload => File.open(path_of_file_to_upload),
        :progress_callback_method => progress_callback_method,
        :overwrite_existing_file => overwrite_existing_file,
        :overwrite_existing_file_only_if_size_mismatch => overwrite_existing_file_only_if_size_mismatch,
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
    # @option args [String] :bucket_name The name of the bucket to be deleted
    # @option args [Boolean] :delete_files Determines a search and deletion of existing files will be executed before
    # the attempt to the delete the bucket
    def delete_bucket(args = { })
      bucket_name = args[:bucket_name]
      raise ArgumentError, ':bucket_name must be set and cannot be empty.' unless bucket_name.respond_to?(:empty?) and !bucket_name.empty?

      delete_files = args[:delete_files]

      if delete_files
        files = storage.directories.get(bucket_name).files.map { |file| file.key }
        storage.delete_multiple_objects(bucket_name, files) unless files.empty?
      end
      storage.delete_bucket(bucket_name)
    end

    # @param [Hash] args
    # @option args [String] :bucket_name The name of the bucket that the file to delete is located in.
    # @option args [String] :object_key The name of the file to delete.
    def delete_object(args = { })
      bucket_name = args[:bucket_name] || args[:bucket] || default_bucket_name
      raise ArgumentError, ':bucket_name must be set and cannot be empty.' unless bucket_name.respond_to?(:empty?) and !bucket_name.empty?

      object_key = args[:object_key]
      object_key = object_key[1..-1] while object_key.start_with?('/') if object_key.respond_to?(:start_with?)
      raise ArgumentError, ':object_key must be set and cannot be empty.' unless object_key.respond_to?(:empty?) and !object_key.empty?

      bucket = storage.directories.new( :key => bucket_name )
      file = bucket.files.new( :key => object_key )
      file.destroy
    end

    def put_object(bucket_name, object_key, file_contents, options = { })
      storage.put_object(bucket_name, object_key, file_contents, options)
    end

    # @param [Hash] args
    # @option args [String] :file_to_upload
    # @option args [String] :path_of_file_to_upload
    # @option args [Boolean] :use_multipart_upload
    def upload(args = { })
      upload_args = process_common_upload_arguments(args)
      file_to_upload = upload_args[:file_to_upload]
      file_size = file_to_upload.size

      allow_multipart_upload     = args.fetch(:allow_multipart_upload, default_multipart_upload_allowed)
      multipart_upload_threshold = args.fetch(:multipart_upload_threshold, default_multipart_upload_threshold)

      return do_upload_multipart(args.merge(upload_args)) if allow_multipart_upload && (file_size >= multipart_upload_threshold)

      existing_file = existing_file_check(upload_args)
      return existing_file if existing_file

      bucket_name = upload_args[:bucket_name]
      object_key = upload_args[:object_key]

      file_upload_start = Time.now
      response = put_object(bucket_name, object_key, file_to_upload, upload_args[:options])
      file_upload_end = Time.now
      file_upload_took  = file_upload_end - file_upload_start
      logger.debug { "Uploaded #{file_size} bytes in #{file_upload_took.round(2)} seconds. #{(file_size/file_upload_took).round(0)} Bps" }
      return response
    ensure
      file_to_upload.close if file_to_upload
    end

    def existing_file_check(args)
      return false if args[:overwrite_existing_file]

      bucket_name = args[:bucket_name]
      object_key = args[:object_key]
      # TODO: Figure out how to update the File's ACL without uploading it again
      logger.debug { "Checking for Existing File. Bucket Name: '#{bucket_name}' Object Key: '#{object_key}'" }
      s3_file = get_file_head(bucket_name, object_key)
      unless s3_file
        logger.debug { 'Existing File Not Found.' }
        return false
      end

      logger.debug { "Existing File Found. #{s3_file.inspect}" }
      unless args[:overwrite_existing_file_only_if_size_mismatch]
        logger.debug { 'Skipping Upload.' }
        return s3_file
      end

      file_to_upload = args[:file_to_upload]
      file_size = file_to_upload.size

      s3_file_content_length = s3_file.content_length
      if s3_file_content_length == file_size
        logger.debug { "Skipping Upload. File Size Match: #{s3_file_content_length} == #{file_size}" }
        return s3_file
      end

      logger.debug { "Not Skipping Upload. File Size Mis-match: #{s3_file_content_length} != #{file_size}" }
      return false
    end

    # @return [Excon]
    def do_upload_multipart(args = { })
      existing_file = existing_file_check(args)
      return existing_file if existing_file

      file                    = args[:file_to_upload]
      bucket_name             = args[:bucket_name]
      object_key              = args[:object_key]
      initiate_upload_options = args[:options]
      progress_callback       = args[:progress_callback_method]
      _multipart_chunk_size   = args[:multipart_chunk_size] || args[:block_size] || default_multipart_upload_chunk_size
      thread_limit            = args[:thread_limit]
      threaded                = args.fetch(:threaded, true)

      mp_upload_args = {
        :logger => logger,
        :file => file,
        :s3 => storage,
        :bucket_name => bucket_name,
        :object_key => object_key,
        :part_size => _multipart_chunk_size,
        :threaded => threaded,
        :initiate_upload_options => initiate_upload_options,
        :progress_callback_method => progress_callback,
      }
      mp_upload_args[:thread_limit] = thread_limit if thread_limit

      mp_upload = MultipartUpload.new(mp_upload_args)
      mp_upload.upload_parts
    end

    def do_upload_multipart_old(args = { })
      existing_file = existing_file_check(args)
      return existing_file if existing_file

      file                  = args[:file_to_upload]
      bucket_name           = args[:bucket_name]
      object_key            = args[:object_key]
      upload_options        = args[:options]
      progress_callback     = args[:progress_callback_method]
      _multipart_chunk_size = args[:multipart_chunk_size] || default_multipart_upload_chunk_size

      response = storage.initiate_multipart_upload(bucket_name, object_key, upload_options)
      upload_id = response.body['UploadId']

      part_tags = []

      file_size = file.size
      total_parts = (file_size.to_f / _multipart_chunk_size.to_f).ceil

      if total_parts > MULTIPART_UPLOAD_PART_LIMIT
        total_parts = MULTIPART_UPLOAD_PART_LIMIT
        _multipart_chunk_size = (file_size / MULTIPART_UPLOAD_PART_LIMIT)
      end

      # Make sure we are reading from the start of the file
      file.rewind if file.respond_to?(:rewind)

      part_counter = 0
      file_upload_start = file_upload_end = nil
      while (chunk = file.read(_multipart_chunk_size)) do
        part_counter += 1
        logger.debug { "Uploading Part #{part_counter} of #{total_parts}" }

        if progress_callback
          # This is for backwards compatibility with the Orchestrator S3 plugin prior to version 1.2.0
          progress_callback_args = if progress_callback.arity >= 3
                                     # Legacy
                                     status = nil
                                     status_detail = "Uploading Part #{part_counter} of #{total_parts}"
                                     percentage = ((part_counter/total_parts) * 100)
                                     [ status, status_detail, percentage ]
                                   else
                                     {
                                       :total_parts => total_parts,
                                       :part_counter => part_counter,
                                       :file_size => file_size
                                     }
                                   end
          progress_callback.call(progress_callback_args)
        end

        md5 = Base64.encode64(Digest::MD5.digest(chunk)).strip
        part_upload_start = Time.now
        part_upload = storage.upload_part(bucket_name, object_key, upload_id, part_tags.size + 1, chunk, 'Content-MD5' => md5 )
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
      storage.abort_multipart_upload(bucket_name, object_key, upload_id) if upload_id
      raise
    else
      return storage.complete_multipart_upload(bucket_name, object_key, upload_id, part_tags)
    ensure
      file.close if file
    end
    
    # Uploads a file in parts
    #
    # +-----------------------------------+---------------------------------------+
    # |Maximum object size                | 5 TB                                  |
    # +-----------------------------------+---------------------------------------+
    # |Maximum number of parts per upload | 10,000                                |
    # +-----------------------------------+---------------------------------------+
    # |Part numbers                       | 1 to 10,000 (inclusive)               |
    # +-----------------------------------+---------------------------------------+
    # |Part size                          | 5 MB to 5 GB, last part can be < 5 MB |
    # +-----------------------------------+---------------------------------------+
    # | http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html                |
    # +---------------------------------------------------------------------------+
    #
    # @param [Hash] args
    # @option args [String] :file_to_upload
    # @option args [String] :bucket_name
    # @option args [String] :object_key
    # @option args (Integer) :multipart_chunk_size (DEFAULT_MULTIPART_CHUNK_SIZE)
    # @option args [Method] :progress_callback_method
    def upload_multipart(args = { })
      upload_args = process_common_upload_arguments(args)
      do_upload_multipart(upload_args)
    end

    def upload_multipart_threaded(args = { })
      raise 'This method has not yet been implemented.'

      existing_file = existing_file_check(args)
      return existing_file if existing_file

      file                  = args[:file_to_upload]
      bucket_name           = args[:bucket_name]
      object_key            = args[:object_key]
      upload_options        = args[:options]
      progress_callback     = args[:progress_callback_method]
      _multipart_chunk_size = args[:multipart_chunk_size] || default_multipart_upload_chunk_size

      response = storage.initiate_multipart_upload(bucket_name, object_key, upload_options)
      upload_id = response.body['UploadId']

      split_work_dir = args[:split_work_dir]




    end

  end

end