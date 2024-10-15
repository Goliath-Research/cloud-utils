import os
from Storage import Storage

class FileSystemStorage(Storage):
    def read(self, path, filesys_path):
        # For file system storage, simply copy the file
        if os.path.exists(filesys_path):
            offset = os.path.getsize(filesys_path)
        else:
            offset = 0

        with open(path, 'rb') as source_file:
            source_file.seek(offset)
            with open(filesys_path, 'ab') as target_file:
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
