################################################################################
import ctypes
import os
import threading
import sys
import time
import unrar

from unrar import unrarlib

################################################################################
class Extractor(threading.Thread):
    ############################################################################
    def __init__(self, rar_path):
        threading.Thread.__init__(self)
        self.rar_path = rar_path
        self.files    = []

    ############################################################################
    def run(self):
        self.stop_requested = False

        while not self.stop_requested and not os.path.isfile(self.rar_path):
            time.sleep(1)

        sys.stdout.write('[nzb2http][extractor] {0} found\n'.format(self.rar_path))

        archive_data   = unrarlib.RAROpenArchiveDataEx(self.rar_path, mode=unrarlib.constants.RAR_OM_EXTRACT)
        archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))  
        
        callback = unrarlib.UNRARCALLBACK(self._callback)

        header_result, header_data = self._read_header(archive_handle)
        while not self.stop_requested and header_result == unrarlib.constants.SUCCESS:
            self.files.append(header_data.FileName)

            with open(header_data.FileName, 'wb') as output_file:
                self.output_file = output_file
                unrarlib.RARSetCallback(archive_handle, callback, ctypes.addressof(ctypes.py_object(output_file)))
                unrarlib.RARProcessFileW(archive_handle, unrarlib.constants.RAR_TEST, None, None)
                self.output_file = None

            header_result, header_data = self._read_header(archive_handle)

        unrarlib.RARCloseArchive(archive_handle)

        sys.stdout.write('[nzb2http][extractor] Extraction complete\n')

    ############################################################################
    def stop(self):
        self.stop_requested = True
        self.join()

    ############################################################################
    def _read_header(self, archive_handle):
        header_data   = unrarlib.RARHeaderDataEx()
        header_result = unrarlib.RARReadHeaderEx(archive_handle, ctypes.byref(header_data))
        return (header_result, header_data)

    ############################################################################
    def _callback(self, msg, userdata, p1, p2):
        if self.stop_requested:
            return -1

        if msg == unrar.constants.UCM_PROCESSDATA:
            self.output_file.write((ctypes.c_char * p2).from_address(p1).raw)
        elif msg == unrar.constants.UCM_CHANGEVOLUME:
            if p2 == unrar.constants.RAR_VOL_NOTIFY:
                sys.stdout.write('[nzb2http][extractor] Extracting from {0}\n'.format(ctypes.c_char_p(p1).value))
        
        return 1
