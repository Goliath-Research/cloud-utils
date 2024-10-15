import os
from Storage import Storage

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
