import os
import re
from Storage import Storage, StorageConfig
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

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

    def _find_files_in_storage(self, path, regex_pattern):
        matching_files = []
        blob_list = self.container_client.list_blobs(name_starts_with=path)
        for blob in blob_list:
            if re.match(regex_pattern, blob.name):
                matching_files.append(blob.name)
        return matching_files
    