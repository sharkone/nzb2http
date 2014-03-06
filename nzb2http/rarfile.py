################################################################################
import ctypes
import io
import sys
import threading
import time
import unrar
import Queue

from unrar import unrarlib

################################################################################
class RarFile(io.RawIOBase):
    ############################################################################
    def __init__(self, path):
        self.path = path

        archive_data   = unrarlib.RAROpenArchiveDataEx(self.path, mode=unrarlib.constants.RAR_OM_LIST_INCSPLIT)
        archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))  
        
        header_result, header_data = self._read_header(archive_handle)
        self.content_file_name = header_data.FileName
        self.content_file_size = header_data.UnpSize
        unrarlib.RARCloseArchive(archive_handle)

        archive_data   = unrarlib.RAROpenArchiveDataEx(self.path, mode=unrarlib.constants.RAR_OM_EXTRACT)
        archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))  
        
        header_result, header_data = self._read_header(archive_handle)
        unrarlib.RARCloseArchive(archive_handle)

        self._start_thread()

    ############################################################################
    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            new_position = offset
        elif whence == io.SEEK_CUR:
            new_position = self.position + offset
        elif whence == io.SEEK_END:
            new_position = self.content_file_size + offset

        if new_position > self.position:
            self.read(new_position - self.position)
        elif new_position < self.position:
            self._start_thread()
            self.read(offset)
        sys.stdout.write('[nzb2http][rarfile] Seeking @ {0} successful\n'.format(new_position))

        if self.position < self.content_file_size:
            self.eof = False

        return self.position

    ############################################################################
    def close(self):
        self.worker.stop()

    ############################################################################
    def read(self, n=-1):
        if n == -1:
            return self.readall()

        if self.eof:
            return ""

        result     = ""
        read_count = 0
        while read_count != n:
            if not self.buffer:
                self.buffer = self.queue.get()
                self.queue.task_done()
                if self.buffer == ['R', 'A', 'R', 'E', 'O', 'F']:
                    self.eof    = True
                    self.buffer = []
                    break

            remaining_size = n - read_count
            buffer_size    = len(self.buffer)
            
            if buffer_size > 0:
                if buffer_size <= remaining_size:
                    result      = result + self.buffer
                    read_count  = read_count + buffer_size
                    self.buffer = []
                else:
                    result      = result + self.buffer[0:remaining_size]
                    read_count  = read_count + remaining_size
                    self.buffer = self.buffer[remaining_size:]

        self.position = self.position + read_count
        return result

    ############################################################################
    def readall(self):
        return self.read(self.get_content_file_size())

    ############################################################################
    def _start_thread(self):
        self.queue    = Queue.Queue(10)
        self.eof      = False
        self.buffer   = []
        self.position = 0

        self.worker = RarFileWorkerThread(self)
        self.worker.start()

    ############################################################################
    def _read_header(self, archive_handle):
        header_data   = unrarlib.RARHeaderDataEx()
        header_result = unrarlib.RARReadHeaderEx(archive_handle, ctypes.byref(header_data))
        return (header_result, header_data)

################################################################################
class RarFileWorkerThread(threading.Thread):
    ############################################################################
    def __init__(self, owner):
        threading.Thread.__init__(self)
        self.daemon = True
        self.owner  = owner

    ############################################################################
    def run(self):
        self.stop_requested = False

        archive_data   = unrarlib.RAROpenArchiveDataEx(self.owner.path, mode=unrarlib.constants.RAR_OM_EXTRACT)
        archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))

        callback = unrarlib.UNRARCALLBACK(self._extract_callback)
        unrarlib.RARSetCallback(archive_handle, callback, 0)

        sys.stdout.write('[nzb2http][rarfile] Extracting from {0}\n'.format(self.owner.path))

        try:
            header_result, header_data = self.owner._read_header(archive_handle)
            while not self.stop_requested and header_result is unrarlib.constants.SUCCESS:
                unrarlib.RARProcessFileW(archive_handle, unrarlib.constants.RAR_TEST, None, None)
                header_result, header_data = self.owner._read_header(archive_handle)
        except unrarlib.UnrarException as exception:
            sys.stdout.write('UnrarException:{0}\n'.format(exception))

        unrarlib.RARCloseArchive(archive_handle)
        self.owner.queue.put(['R', 'A', 'R', 'E', 'O', 'F'])

    ############################################################################
    def stop(self):
        self.stop_requested = True
        self.join()

    ############################################################################
    def _extract_callback(self, msg, userdata, p1, p2):
        if self.stop_requested:
            return -1

        if msg == unrar.constants.UCM_PROCESSDATA:
            while True:
                if self.stop_requested:
                    return -1
                try:
                    self.owner.queue.put((ctypes.c_char * p2).from_address(p1).raw, True, 1)
                    break
                except Queue.Full:
                    pass
        elif msg == unrar.constants.UCM_CHANGEVOLUME:
            if p2 == unrar.constants.RAR_VOL_ASK:
                sys.stdout.write('[nzb2http][rarfile] Waiting for {0}\n'.format(ctypes.c_char_p(p1).value))
                time.sleep(1)
            elif p2 == unrar.constants.RAR_VOL_NOTIFY:
                sys.stdout.write('[nzb2http][rarfile] Extracting from {0}\n'.format(ctypes.c_char_p(p1).value))
        
        return 1
