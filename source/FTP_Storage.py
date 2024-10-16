import os
import re
from ftplib import FTP
from Storage import Storage, StorageConfig

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

    def _find_files_in_storage(self, path, regex_pattern):
        matching_files = []
        def recursive_ftp_walk(current_path):
            try:
                self.ftp.cwd(current_path)
                items = self.ftp.nlst()
                for item in items:
                    try:
                        self.ftp.cwd(item)  # If successful, it's a directory
                        recursive_ftp_walk(os.path.join(current_path, item))
                    except Exception:
                        # If cwd fails, it's a file
                        if re.match(regex_pattern, item):
                            matching_files.append(os.path.join(current_path, item))
            except Exception as e:
                print(f"Error while walking FTP path {current_path}: {e}")
        
        recursive_ftp_walk(path)
        return matching_files
