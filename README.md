Ubiquity Amazon S3 Library and Command Line Utilities
=====================================================

Installation
------------

Setup
-----

Amazon S3 Upload Executable [bin/ubiquity-s3-upload](./bin/ubiquity-s3-upload)
------------------------------------------------------------------------------
An executable to facilitate the uploading of files to the S3 Amazon Web Service

Usage: ubiquity-s3-upload [options]
        
    --aws-key KEY                The Amazon Web Services (AWS) Access Key ID.
    --aws-secret SECRET          The AWS Secret Access Key.
    --aws-region REGION          The AWS Region to access.
    --bucket NAME                The name of the bucket to save the file to.
    --object-key KEY             The unique name to use when saving the file to the bucket.
    --file-to-upload PATH        The path of the file to upload.
    --[no-]use-multipart-upload  Determines if multipart upload will be used to upload the file
    --multipart-chunk-size BYTES Determines the size of each chunk in a multipart upload.
    --thread-limit NUM           Determines the maximum number concurrent of threads to use when performing  a multipart upload.
    
#### Usage Examples:
    
###### Accessing Help
    ./ubiquity-s3-upload --help
    
###### General Upload
    ./ubiquity-s3-upload --aws-key <KEY> --aws-secret <SECRET> --bucket <BUCKET NAME> --file-to-upload "PATH"
    
    