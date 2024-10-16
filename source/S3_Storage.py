from Storage import Storage, StorageConfig
import re
import os
import boto3

class S3Storage(Storage):
    def __init__(self, config: StorageConfig):
        if config.auth_method == 'oauth':
            # Use OAuth token for authentication
            self.s3 = boto3.client(
                's3', 
                config.access_key_id,
                config.secret_access_key
            )
        else:
            # Use default authentication
            self.s3 = boto3.client('s3')
        self.bucket_name = config.bucket_name

    def read(self, path, local_path):
        # Check if partial file exists
        if os.path.exists(local_path):
            offset = os.path.getsize(local_path)
        else:
            offset = 0

        with open(local_path, 'ab') as f:
            while True:
                try:
                    response = self.s3.get_object(Bucket=self.bucket_name, Key=path, Range=f'bytes={offset}-')
                    chunk = response['Body'].read()
                    if not chunk:
                        break
                    f.write(chunk)
                    offset += len(chunk)
                except self.s3.exceptions.NoSuchKey:
                    raise FileNotFoundError(f"The file {path} does not exist in bucket {self.bucket_name}")
                except Exception as e:
                    print(f"Error while reading from S3: {e}")
                    break

    def write(self, path, data):
        # Split data into chunks and upload with retry capability
        chunk_size = 1024 * 1024 * 5  # 5MB chunks
        total_size = len(data)
        part_number = 1
        multipart_upload = self.s3.create_multipart_upload(Bucket=self.bucket_name, Key=path)
        parts = []

        try:
            for offset in range(0, total_size, chunk_size):
                chunk = data[offset:offset + chunk_size]
                response = self.s3.upload_part(
                    Bucket=self.bucket_name,
                    Key=path,
                    PartNumber=part_number,
                    UploadId=multipart_upload['UploadId'],
                    Body=chunk
                )
                parts.append({
                    'ETag': response['ETag'],
                    'PartNumber': part_number
                })
                part_number += 1

            # Complete multipart upload
            self.s3.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=path,
                UploadId=multipart_upload['UploadId'],
                MultipartUpload={'Parts': parts}
            )
        except Exception as e:
            self.s3.abort_multipart_upload(Bucket=self.bucket_name, Key=path, UploadId=multipart_upload['UploadId'])
            raise e

    def delete(self, path):
        return self.retry_operation(lambda: self.s3.delete_object(Bucket=self.bucket_name, Key=path))

    def _find_files_in_storage(self, path, regex_pattern):
        matching_files = []
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=path)
        if 'Contents' in response:
            for obj in response['Contents']:
                if re.match(regex_pattern, obj['Key']):
                    matching_files.append(obj['Key'])
        return matching_files
    
if __name__ == '__main__':
    my_secret_access_key = r'1JFvNKLTzOx/isf+bDzuN6XSk/0H4QjCxTm8njDg'
    my_access_key_ID = 'AKIA4JUWHVLAZXBG6NIZ'
    my_bucket_name = 'rawdata_samples'

    config = StorageConfig(
        auth_method='oauth',
        access_key_id='AKIA4JUWHVLAZXBG6NIZ',
        secret_access_key='1JFvNKLTzOx/isf+bDzuN6XSk/0H4QjCxTm8njDg',
        bucket_name=my_bucket_name
    )

    s3s = S3Storage(config)
