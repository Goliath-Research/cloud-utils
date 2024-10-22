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

        def ftp_walk(current_path):
            items = self.ftp.nlst()
            for item in items:
                try:
                    new_path = os.path.join(current_path, item).replace('\\', '/')

                    # Attempt to get the size of the item, if it fails, it's a directory
                    self.ftp.size(item)

                    # If size() succeeds, it's a file
                    if re.match(regex_pattern, item):
                        matching_files.append(new_path)
                except Exception:
                    # If size() fails, it's a directory, so walk it recursively
                    self.ftp.cwd(item)
                    ftp_walk(new_path) 

        ftp_path = os.path.normpath(path).replace('\\', '/')
        self.ftp.cwd(ftp_path)

        ftp_walk(ftp_path)
        return matching_files

if __name__ == "__main__":
    config = StorageConfig(
        auth_method="username_password",
        username="anonymous",
        password="dizada@epimethyl.com",
        host="ftp.sra.ebi.ac.uk"
    )

    ftp = FTPStorage(config)

    fastq_files = ftp.find_files("vol1/fastq/SRR811", r".*\.(fq|fastq)\.gz$")
    for fq in fastq_files:
        test, sample = os.path.basename(fq), os.path.basename(os.path.dirname(fq))
        ftp.read(fq, os.path.normpath(os.path.join(sample, test)))
