from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Optional


class StorageConfig(BaseModel):
    auth_method: (
        str  # 'username_password', 'access_key', 'oauth', 'connection_string', 'none'
    )
    username: Optional[str] = None
    password: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    oauth_token: Optional[str] = None
    connection_string: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    container_name: Optional[str] = None
    bucket_name: Optional[str] = None

class Storage(ABC):
    """
    Abstract class for storage operations.
    Concrete implementations should inherit from this class and implement the read, write, delete, and _find_files_in_storage methods.
    Concrete implementations should also implement the __init__ method to initialize the storage client.
    Currently, the following concrete implementations are available:
    1. FileSystemStorage
    2. FTPStorage
    3. SFTPStorage
    4. S3Storage
    5. AzureBlobStorage
    The StorageConfig class is used to pass configuration parameters to the storage client.
    """

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

    def find_files(self, path, regex_pattern):
        return self._find_files_in_storage(path, regex_pattern)

    @abstractmethod
    def _find_files_in_storage(self, path, regex_pattern):
        pass
