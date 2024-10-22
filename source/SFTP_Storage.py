import os
import re
from Storage import Storage, StorageConfig
import paramiko

class SFTPStorage(Storage):
    ''''SFTP storage class'''
    
    def __init__(self, config: StorageConfig):
        self.transport = paramiko.Transport((config.host, config.port))
        if config.auth_method == 'username_password':
            self.transport.connect(username=config.username, password=config.password)
        else:
            self.transport.connect(username='anonymous', password='someone@mail.com')

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
    
    def _find_files_in_storage(self, path, regex_pattern):
        matching_files = []
        def recursive_sftp_walk(current_path):
            try:
                items = self.sftp.listdir(current_path)
                for item in items:
                    item_path = os.path.join(current_path, item)
                    if self._is_directory(item_path):
                        recursive_sftp_walk(item_path)
                    else:
                        if re.match(regex_pattern, item):
                            matching_files.append(item_path)
            except Exception as e:
                print(f"Error while walking SFTP path {current_path}: {e}")
        
        def _is_directory(self, path):
            try:
                return paramiko.S_ISDIR(self.sftp.stat(path).st_mode)
            except Exception:
                return False
        
        recursive_sftp_walk(path)
        return matching_files
