import os
from ftplib import FTP
from Storage import Storage, StorageConfig
import paramiko

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