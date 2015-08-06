require 'logger'

require 'ubiquity/s3/multipart_upload/chunked_file'
require 'ubiquity/s3/multipart_upload/part_upload'

module Ubiquity

  class S3

    class MultipartUpload

      BYTE_UNITS = %w(B KB MB GB TB PB EB ZB YB BB)
      DEFAULT_THREAD_LIMIT = 5
      DEFAULT_PART_SIZE = 5242880 # 5 * 1024 * 1024 # 5MB
      MULTIPART_UPLOAD_PART_LIMIT = 10000

      attr_accessor :logger, :progress_callback_method

      attr_reader :file, :part_size, :s3, :bucket, :bucket_name, :object_key, :initiate_upload_options, :thread_limit

      attr_reader :active_thread_count, :bytes_uploaded, :complete_upload_response, :initiate_upload_response,
                  :part_etags, :threads, :time_started, :time_ended, :upload_id, :uploads


      def initialize(args = { })
        args = args.dup
        initialize_logger(args)

        @part_size = args[:part_size] || DEFAULT_PART_SIZE
        if part_size < DEFAULT_PART_SIZE
          logger.warn { 'Part Size Must Be 5242880 (5MB) or Greater. Part Size has Been Reset to the Default Value.' }
          @part_size = DEFAULT_PART_SIZE
        end

        @file = args[:file]
        unless file.is_a?(ChunkedFile)
          chunked_file_args = args
          chunked_file_args[:chunk_size] = part_size
          chunked_file_args[:maximum_chunks] = MULTIPART_UPLOAD_PART_LIMIT
          @file = ChunkedFile.new(chunked_file_args)
        end

        @s3 = args[:s3]
        s3.sync_clock

        @bucket = args[:bucket]
        @bucket_name = args[:bucket_name]
        if bucket
          @bucket_name = bucket.name unless bucket_name
        else
          #@bucket = s3.bucket(bucket_name) if bucket_name
        end

        @object_key = args[:object_key]
        @initiate_upload_options = args[:initiate_upload_options] || { }

        @progress_callback_method = args[:progress_callback_method]

        self.thread_limit = args[:thread_limit] || DEFAULT_THREAD_LIMIT
        @threaded = args.fetch(:threaded, false)

        #Thread.abort_on_exception = false if threaded?
        @time_started = nil
        @time_ended = nil
        @time_elapsed = nil

        @active_thread_count = 0
        @bytes_uploaded = 0
        @uploads = { }
        @threads = [ ]
        @part_etags = { }
        @completed_response = nil

        upload_parts if args[:auto_start_transfer]
      end

      def initialize_logger(args = { })
        @logger = args[:logger] ||= begin
          _logger = Logger.new(args[:log_to] || STDOUT)
          _logger.level = args[:level] || Logger::DEBUG
          _logger
        end
      end

      def abort
        logger.debug { "Aborting Upload of '#{file.path}' to '#{File.join(bucket_name, object_key)}'. Upload Id: #{upload_id}" }
        @aborted = true
        s3.abort_multipart_upload(bucket_name, object_key, upload_id) if upload_id
      end

      def aborted?
        @aborted
      end

      def completed?
        !!@complete_upload_response
      end

      def bytes_per_second
        file.size / time_elapsed
      end

      def bytes_per_second_humanized
        humanize_bytes(bytes_per_second)
      end

      def bytes_remaining
        file.size - bytes_uploaded
      end

      def bytes_remaining_humanized
        humanize_bytes(bytes_remaining)
      end

      def bytes_uploaded_humanized
        humanize_bytes(bytes_uploaded)
      end

      def estimated_time_at_completion
        @time_ended || (Time.now + estimated_time_remaining)
      end

      def estimated_time_remaining
        bytes_remaining / bytes_per_second
      end

      def file_size_as_float
        @file_size_as_float ||= file.size.to_f
      end

      def humanize_bytes(bytes, options = { })
        return "#{bytes}B" if bytes < 1

        kilo_size = options[:kilo_size] || 1024
        rounding = options[:rounding] || 2

        index = ( Math.log( bytes ) / Math.log( 2 ) ).to_i / 10
        "#{(bytes.to_f / ( kilo_size ** index )).round(rounding) } #{BYTE_UNITS[index]}"
      rescue => e
        raise e, "Exception Humanizing Bytes: #{bytes} : #{e.message}"
      end

      #BYTE_UNITS2 = [[1073741824, 'GB'], [1048576, 'MB'], [1024, 'KB'], [0, 'B']]
      # def humanize_bytes(n)
      #   unit = BYTE_UNITS2.detect{ |u| n > u[0] }
      #   "#{n/unit[0]} #{unit[1]}"
      # end

      # @return [Excon::Response]
      def multipart_upload_complete
        #part_etag_values = tags.values
        part_etag_values = part_etags.sort.map { |part_number,v| v } # Quick fix to fix unordered hash issue with Ruby MRI 1.8.7

        #logger.debug { "Completing Multipart Upload. Bucket Name: '#{bucket_name}' Object Key: #{object_key} Upload Id: '#{upload_id}' Tag Values: #{tag_values.inspect}"}
        logger.debug { "Completing Multipart Upload. Bucket Name: '#{bucket_name}' Object Key: '#{object_key}' Upload Id: '#{upload_id}'"}
        @complete_upload_response = s3.complete_multipart_upload(bucket_name, object_key, upload_id, part_etag_values)
        logger.info { "Completed Upload of '#{file.path}' to '#{File.join(bucket_name, object_key)}' Size: #{file.size} Parts: #{total_parts} @ #{humanize_bytes(part_size)} each. Thread Limit: #{threaded? ? thread_limit : 1} Time Elapsed: #{time_elapsed} seconds @ #{bytes_per_second_humanized}ps" }
        complete_upload_response
      end

      # @return [Excon::Response]
      def multipart_upload_initiate
        logger.info { "Initiating Upload of '#{file.path}' to '#{File.join(bucket_name, object_key)}' Size: #{file.size} Parts: #{total_parts} @ #{humanize_bytes(part_size)} each. Thread Limit: #{threaded? ? thread_limit : 1}" }
        @initiate_upload_response = s3.initiate_multipart_upload(bucket_name, object_key, initiate_upload_options)
        @upload_id = @initiate_upload_response.body['UploadId']
        logger.debug { "Initiated Upload  of '#{file.path}' to '#{File.join(bucket_name, object_key)}'. Upload Id: #{upload_id}" }
        initiate_upload_response
      end

      def percentage_remaining
        ((bytes_remaining / file_size_as_float) * 100)
      end

      def percentage_completed
        ((bytes_uploaded / file_size_as_float) * 100)
      end

      def time_elapsed
        ((@time_ended || Time.now) - @time_started)
      end

      def total_parts
        file.total_chunks
      end

      def status_as_hash
        {
          :size => file.size,
          :bytes_uploaded => bytes_uploaded,
          :bytes_remaining => bytes_remaining,
          :time_started => time_started,
          :time_elapsed => time_elapsed,
          :bytes_per_second => bytes_per_second,
          :active_thread_count => active_thread_count,
          :total_parts => total_parts,
          :parts_uploaded => part_etags.length,
          :estimated_time_remaining => estimated_time_remaining,
          :part_size => part_size,
          :aborted => aborted?,
          :completed => completed?,
          :successful => successful?,
          :threaded => threaded?,
          :thread_limit => thread_limit
        }
      end

      def status_as_string
        "Total Bytes Uploaded: #{bytes_uploaded_humanized} (#{percentage_completed.round(2)}%) in #{time_elapsed.round(2)} seconds @ #{bytes_per_second_humanized}ps Bytes Remaining: #{bytes_remaining_humanized} (#{percentage_remaining.round(2)}%) ETR: #{estimated_time_remaining.ceil} seconds #{estimated_time_at_completion}"
      end

      def store_part_etag(part_number, etag)
        logger.debug { "Adding ETag for Part Number: #{part_number} => #{etag}" }
        part_etags[part_number] = etag
      end

      def successful?
        completed? and @complete_upload_response.body['ETag'] rescue false
      end

      def thread_limit=(new_thread_limit)
        @thread_limit = new_thread_limit < 1 ? 1 : new_thread_limit
      end

      def threaded?
        @threaded
      end

      # Common code used for multi-threaded and not multi-threaded upload_part
      def _upload_part(part)
        logger.debug { "Starting Upload of Part #{part.part_number}. #{part.inspect}" }
        part.do_upload
        @bytes_uploaded += part.size
        logger.debug { "Finished Upload of Part #{part.part_number} in #{part.time_elapsed.round(2)} seconds. #{humanize_bytes(part.bytes_per_second)}ps | #{part.inspect}" }

        logger.debug { status_as_string }

        progress_callback_method.call(status_as_hash) if progress_callback_method

        store_part_etag(part.part_number, part.etag)
        part
      end

      # Triggers the upload of a part
      # If threaded is false then the method will wait for the upload to complete before returning
      # otherwise it will start the upload in a new thread and add the thread to the active threads
      #
      # @return [PartUpload] The Upload Handler Object that Handled the Upload of the Part
      def upload_part(args = { })

        part_upload = PartUpload.new(args)
        return part_upload if aborted?

        # I decide not to store the uploads in an hash to be able to interrogate them later
        #uploads[part_upload.index] = part_upload

        return _upload_part(part_upload) unless threaded?

        #logger.debug { "Incrementing Active Thread Count. #{active_thread_count} + 1" }
        @active_thread_count += 1
        logger.debug { "Active Thread Count Incremented to #{active_thread_count} for Part #{part_upload.part_number}" }
        threads << Thread.new(part_upload) do |_part|
          begin
            _upload_part(_part)
          rescue => e
            logger.error { "Exception in thread #{_part.index} Part Number: #{_part.part_number} Exception: #{e.inspect} #{e.backtrace.inspect}" }
            abort unless _part.successful?
            raise e
          ensure
            #logger.debug { "Decrementing Active Thread Count. #{active_thread_count} - 1" }
            @active_thread_count -= 1
            logger.debug { "Active Thread Count Decremented to #{active_thread_count} for Part #{_part.part_number}" }
          end
        end

        part_upload
      end

      def upload_parts
        multipart_upload_initiate

        part_args = { :s3 => s3, :bucket_name => bucket_name, :object_key => object_key, :upload_id => upload_id }

        @time_started = Time.now
        file.each_with_index do |idx, chunk_contents|
          sleep 0.5 while !aborted? and active_thread_count >= thread_limit

          break if aborted?

          logger.debug { "Starting Upload of Part #{idx+1} of #{file.total_chunks} Active Thread Count: #{@active_thread_count}" }
          upload_part(part_args.merge( :index => idx, :data => chunk_contents ))
        end

        # Make sure all of our threads have finished before we continue
        threads.each do |t|
          begin
            t.join
          rescue => e
            logger.error { "Failed to Join Thread: #{e.message} #{e.backtrace}" }
          end
        end
        @time_ended = Time.now
        logger.debug { "Transfer Completed in #{time_elapsed.round(2)} seconds. #{humanize_bytes(bytes_per_second)}ps"}

        # Make sure we still have a connection before sending the completion request
        s3.reload

        multipart_upload_complete
        return complete_upload_response
      ensure
        abort unless successful?
      end

      # MultipartUpload
    end

    # S3
  end

  # Ubiquity
end