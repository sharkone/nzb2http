################################################################################
import io
import os
import sys
import time

################################################################################
VIRTUAL_READ_THRESHOLD = 100 * 1024

################################################################################
class FileWrapper(io.RawIOBase):
    def __init__(self, path, complete_size):
        self.path          = path
        self.complete_size = complete_size
        self.file          = open(self.path, 'rb')
        self.virtual_read  = False

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            new_position = offset
        elif whence == io.SEEK_CUR:
            new_position = self.file.tell() + offset
        elif whence == io.SEEK_END:
            new_position = self.complete_size + offset

        if new_position > os.path.getsize(self.path):
            if (self.complete_size - new_position) < VIRTUAL_READ_THRESHOLD:
                self.virtual_read = True
                return

            while new_position > os.path.getsize(self.path):
                time.sleep(1)
            return self.file.seek(new_position, io.SEEK_SET)
        else:
            return self.file.seek(new_position, io.SEEK_SET)
        
    def read(self, size=-1):
        if self.virtual_read:
            self.virtual_read = False
            return ""

        if size == -1:
            size = complete_size - self.file.tell()

        while (self.file.tell() + size) > os.path.getsize(self.path):
            time.sleep(1)

        return self.file.read(size)

    def close(self):
        return self.file.close()
