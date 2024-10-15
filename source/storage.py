from abc import ABC, abstractmethod
import os
import boto3
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import paramiko
from ftplib import FTP
from pydantic import BaseModel, Field
from typing import Optional

class StorageConfig(BaseModel):
    auth_method: str
    username: Optional[str] = None
    password: Optional[str] = None
    oauth_token: Optional[str] = None
    connection_string: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    container_name: Optional[str] = None
    bucket_name: Optional[str] = None

class Storage(ABC):
    @abstractmethod
    def read(self, path, local_path):
        pass

    @abstractmethod
    def write(self, path, data):
        pass

    @abstractmethod
    def delete(self, path):
        pass

    def retry_operation(self, func, *args, retries=3):
        for attempt in range(retries):
            try:
                return func(*args)
            except Exception as e:
                if attempt < retries - 1:
                    continue
                else:
                    raise e

class S3Storage(Storage):
    def __init__(self, config: StorageConfig):
        if config.auth_method == 'oauth':
            # Use OAuth token for authentication
            self.s3 = boto3.client('s3', aws_session_token=config.oauth_token)
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

class LocalStorage(Storage):
    def read(self, path, local_path):
        # For local storage, simply copy the file
        if os.path.exists(local_path):
            offset = os.path.getsize(local_path)
        else:
            offset = 0

        with open(path, 'rb') as source_file:
            source_file.seek(offset)
            with open(local_path, 'ab') as target_file:
                while True:
                    chunk = source_file.read(1024 * 1024)  # Read in 1MB chunks
                    if not chunk:
                        break
                    target_file.write(chunk)

    def write(self, path, data):
        with open(path, 'wb') as f:
            f.write(data)

    def delete(self, path):
        os.remove(path)

class AzureBlobStorage(Storage):
    def __init__(self, config: StorageConfig):
        if config.auth_method == 'oauth':
            # OAuth authentication is not natively supported by azure-storage-blob
            raise NotImplementedError("OAuth authentication is not supported for Azure Blob Storage")
        self.blob_service_client = BlobServiceClient.from_connection_string(config.connection_string)
        self.container_client = self.blob_service_client.get_container_client(config.container_name)

    def read(self, path, local_path):
        # Check if partial file exists
        if os.path.exists(local_path):
            offset = os.path.getsize(local_path)
        else:
            offset = 0

        with open(local_path, 'ab') as f:
            while True:
                try:
                    blob_client = self.container_client.get_blob_client(path)
                    stream = blob_client.download_blob(offset=offset)
                    chunk = stream.readall()
                    if not chunk:
                        break
                    f.write(chunk)
                    offset += len(chunk)
                except Exception as e:
                    print(f"Error while reading from Azure Blob: {e}")
                    break

    def write(self, path, data):
        blob_client = self.container_client.get_blob_client(path)
        chunk_size = 1024 * 1024 * 4  # 4MB chunks
        total_size = len(data)
        offset = 0

        while offset < total_size:
            chunk = data[offset:offset + chunk_size]
            try:
                blob_client.upload_blob(chunk, length=len(chunk), overwrite=True, if_match='*')
            except Exception as e:
                print(f"Error while writing chunk to Azure Blob: {e}")
                break
            offset += chunk_size

    def delete(self, path):
        blob_client = self.container_client.get_blob_client(path)
        return self.retry_operation(lambda: blob_client.delete_blob())

class FTPStorage(Storage):
    def __init__(self, config: StorageConfig):
        self.ftp = FTP(config.host)
        if config.auth_method == 'username_password':
            self.ftp.login(user=config.username, passwd=config.password)
        elif config.auth_method == 'none':
            self.ftp.login()

    def read(self, path, local_path):
        # Check if partial file exists
        if os.path.exists(local_path):
            offset = os.path.getsize(local_path)
        else:
            offset = 0

        with open(local_path, 'ab') as f:
            def callback(data):
                f.write(data)

            try:
                self.ftp.retrbinary(f'RETR {path}', callback, rest=offset)
            except Exception as e:
                print(f"Error while reading from FTP: {e}")

    def write(self, path, data):
        with open(data, 'rb') as f:
            try:
                self.ftp.storbinary(f'STOR {path}', f)
            except Exception as e:
                print(f"Error while writing to FTP: {e}")

    def delete(self, path):
        return self.retry_operation(lambda: self.ftp.delete(path))

class SFTPStorage(Storage):
    def __init__(self, config: StorageConfig):
        self.transport = paramiko.Transport((config.host, config.port))
        if config.auth_method == 'username_password':
            self.transport.connect(username=config.username, password=config.password)
        elif config.auth_method == 'oauth':
            raise NotImplementedError("OAuth authentication is not supported for SFTP")
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def read(self, path, local_path):
        # Check if partial file exists
        if os.path.exists(local_path):
            offset = os.path.getsize(local_path)
        else:
            offset = 0

        with open(local_path, 'ab') as f:
            try:
                with self.sftp.open(path, 'rb') as remote_file:
                    remote_file.seek(offset)
                    while True:
                        chunk = remote_file.read(1024 * 1024)  # Read in 1MB chunks
                        if not chunk:
                            break
                        f.write(chunk)
            except Exception as e:
                print(f"Error while reading from SFTP: {e}")

    def write(self, path, data):
        with self.sftp.open(path, 'wb') as remote_file:
            offset = 0
            chunk_size = 1024 * 1024  # 1MB chunks
            total_size = len(data)

            while offset < total_size:
                chunk = data[offset:offset + chunk_size]
                try:
                    remote_file.write(chunk)
                except Exception as e:
                    print(f"Error while writing to SFTP: {e}")
                    break
                offset += chunk_size

    def delete(self, path):
        return self.retry_operation(lambda: self.sftp.remove(path))