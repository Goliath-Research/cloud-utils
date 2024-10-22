import os
import re
from Storage import Storage, StorageConfig
from azure.storage.blob import BlobServiceClient

class AzureBlobStorage(Storage):
    '''
    This class implements the abstract Storage class for Azure Blob Storage
    '''
    def __init__(self, config: StorageConfig):
        # Assign the BlobService client
        if config.auth_method == "connection_string":
            self.blob_service_client = BlobServiceClient.from_connection_string(config.connection_string)
        else:
            raise Exception("AzureBlob requires connection_string authentication method!")

        # Assign the container client
        self.container_client = self.blob_service_client.get_container_client(config.container_name)

        # Check if the container exists
        if not self.container_client.exists():
            raise Exception("The account is valid, but the container doesn't exist!")

    def read(self, path, local_path):
        # Check if partial file exists
        if os.path.exists(local_path):
            offset = os.path.getsize(local_path)
        else:
            offset = 0

        with open(local_path, "ab") as f:
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
        # Create the container if it doesn't exist
        self.container_client.create_if_not_exists()

        # Assign the blob client
        blob_client = self.container_client.get_blob_client(path)

        chunk_size = 1024 * 1024 * 4  # 4MB chunks
        total_size = len(data)
        offset = 0

        while offset < total_size:
            chunk = data[offset : offset + chunk_size]
            try:
                blob_client.upload_blob(
                    chunk, 
                    length=len(chunk), 
                    overwrite=True
                )
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

if __name__ == "__main__":
    connection_string = (
        "DefaultEndpointsProtocol=https;"
        "AccountName=blobsamples1;"
        "AccountKey=UHf6qpSbrwg4kOMhivPHIunqtY7s9YknDX6mt6AdE/r21dA21Lhz0dXx0AuJN0HZ3EEDkc2tiZqM+AStqnsVDw==;"
        "EndpointSuffix=core.windows.net"
    )

    config = StorageConfig(
        auth_method="connection_string",
        connection_string=connection_string,
        container_name="blobsamples1"
    )

    ab = AzureBlobStorage(config)
    fastq_files = ab.find_files("H202SC24052442/01.RawData", r".*\.fq.gz$")
    for fq in fastq_files:
        test, sample = os.path.basename(fq), os.path.basename(os.path.dirname(fq))
        ab.read(fq, os.path.join(sample, test))
