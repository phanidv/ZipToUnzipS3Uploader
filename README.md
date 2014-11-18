ZipToUnzipS3Uploader
====================
Utility class to load unzipped files from the zip files present in a S3 bucket in AWS 

Unzips the zip files from the given bucket [bucket_name] and then loads the unzipped files onto the newly created bucket "[bucket_name].unzipped" in a similar folder structure.
Accesskey and Secret key of the user's AWS account should also be provided through commandline, in addition to bucket name.

Usage: ZipToUnzipS3FileUploader <access_key> <secret_key> <bucket_name>

Unzipping code from our virtual "guru" MKyong
