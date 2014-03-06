################################################################################
import extractor
import multiprocessing
import nntp
import os
import pynzb
import re
import sys
import threading
import yenc
import zlib

from multiprocessing.pool import ThreadPool

################################################################################
RE_NZB_FILE_NAME = re.compile('\"(.+)\"')

RE_RAR      = re.compile('\.rar$')
RE_RXX      = re.compile('\.r\d+$')
RE_XXX      = re.compile('\.\d\d\d$')
RE_PART_XX  = re.compile('.part\d+.rar$')
RE_PART_01  = re.compile('.part01.rar$')
RE_001      = re.compile('.001$')

WORKERS = {}

################################################################################
def _init_worker(nntp_credentials):
    WORKERS[threading.current_thread()] = nntp.NNTP(nntp_credentials['host'], nntp_credentials['port'], nntp_credentials['username'], nntp_credentials['password'], nntp_credentials['use_ssl'])

################################################################################
def _run_worker(nzb_segment):
    response, number, message_id, text_lines = WORKERS[threading.current_thread()].article('<' + nzb_segment.message_id + '>')
    return _yenc_decode(text_lines)

############################################################################
def _yenc_decode(text_lines):
    RE_YENC_BEGIN = re.compile('=ybegin .*name=(.+)')
    RE_YENC_PART  = re.compile('=ypart .*begin=(.+) ')

    yenc_decoder        = None
    content_file_name   = None
    content_file_offset = 0

    for text_line in text_lines:
        if not yenc_decoder:
            match_result = RE_YENC_BEGIN.match(text_line)
            if match_result:
                content_file_name = match_result.group(1)
                yenc_decoder = yenc.Decoder()
            continue
        if text_line.startswith('=ypart '):
            content_file_offset = int(RE_YENC_PART.match(text_line).group(1)) - 1
            continue
        if text_line.startswith('=yend '):
            break

        yenc_decoder.feed(text_line)

    return yenc_decoder.getDecoded()

################################################################################
class Downloader(threading.Thread):
    ############################################################################
    def __init__(self, nntp_credentials, download_dir, nzb_name, nzb_content):
        threading.Thread.__init__(self)
        self.nntp_credentials = nntp_credentials
        self.download_dir     = download_dir
        self.nzb_name         = nzb_name
        self.nzb_dir          = os.path.join(self.download_dir, nzb_name[:-4])

        sys.stdout.write('[nzb2http][downloader] Downloading {0}\n'.format(nzb_name))
        self.nzb_files = pynzb.nzb_parser.parse(nzb_content)
        for nzb_file in self.nzb_files:
            nzb_file.name  = RE_NZB_FILE_NAME.search(nzb_file.subject).group(1)
            nzb_file.path  = os.path.join(self.nzb_dir, nzb_file.name)

        self.incomplete_files = list(self.nzb_files)

        sfv_files = self._get_files(self.nzb_files, '.sfv')
        if sfv_files and os.path.isfile(sfv_files[0].path):
            sys.stdout.write('[nzb2http][downloader] - Verifying completeness using sfv file: {0}\n'.format(sfv_files[0].name))
            complete_files, incomplete_files = self._parse_sfv_file(sfv_files[0].path, self.nzb_files)
            for complete_file in complete_files:
               sys.stdout.write('[nzb2http][downloader] - Complete: {0} ({1})\n'.format(complete_file.name, complete_file.crc32))
            self.incomplete_files = incomplete_files
        else:
            sys.stdout.write('[nzb2http][downloader] - Verifying completeness using file presence\n')
            for nzb_file in self.nzb_files:
                if os.path.isfile(nzb_file.path):
                    sys.stdout.write('[nzb2http][downloader] - Complete: {0}\n'.format(nzb_file.name))
                    self.incomplete_files.remove(nzb_file)

        self.incomplete_files = self._sort_files(self.incomplete_files)

        sys.stdout.write('[nzb2http][downloader] {0} files will be downloaded in order:\n'.format(len(self.incomplete_files)))
        for incomplete_file in self.incomplete_files:
           sys.stdout.write('[nzb2http][downloader] - {0}\n'.format(incomplete_file.name))

        self.pool = ThreadPool(self.nntp_credentials['max_connections'], _init_worker, (self.nntp_credentials,))

        self.extractor = extractor.Extractor(self.get_first_rar_path())
        self.extractor.start()

    ############################################################################
    def run(self):
        sys.stdout.write('[nzb2http][downloader] Started\n')
        self.stop_requested = False

        for incomplete_file in self.incomplete_files:
            map_result_async = self.pool.map_async(_run_worker, incomplete_file.segments)
            while not self.stop_requested:
                try:
                    map_result = map_result_async.get(1)
                    sys.stdout.write('[nzb2http][downloader] Downloaded {0}\n'.format(incomplete_file.path))
                    self._write_nzb_file(incomplete_file, map_result)
                    break
                except multiprocessing.TimeoutError:
                    pass

        self.pool.terminate()
        self.pool.join()

        sys.stdout.write('[nzb2http][downloader] Stopped\n')

    ############################################################################
    def stop(self):
        sys.stdout.write('[nzb2http][downloader] Stopping\n')
        
        if self.extractor:
            self.extractor.stop()

        self.stop_requested = True
        self.join()

    ############################################################################
    def get_first_rar_path(self):
        return self._get_first_rar_file(self.nzb_files).path

    ############################################################################
    def _sort_files(self, nzb_files):
        sorted_files = []
        sorted_files = sorted_files + self._get_files(nzb_files, '.sfv')
        sorted_files = sorted_files + self._get_rar_files(nzb_files)
        sorted_files = sorted_files + self._get_files(nzb_files, '.par2')
        sorted_files = sorted_files + self._get_remaining_files(nzb_files, sorted_files)
        return sorted_files

    ############################################################################
    def _get_files(self, nzb_files, pattern):
        files = []
        for nzb_file in nzb_files:
            if nzb_file.name.endswith(pattern):
                files.append(nzb_file)
        files.sort(key=lambda file: file.name)
        return files

    ############################################################################
    def _get_first_rar_file(self, nzb_files):
        for nzb_file in nzb_files:
            if RE_RAR.search(nzb_file.name) or RE_RXX.search(nzb_file.name) or RE_XXX.search(nzb_file.name):
                if (RE_RAR.search(nzb_file.name) or RE_PART_01.search(nzb_file.name) or RE_001.search(nzb_file.name)) and not 'subs' in nzb_file.name:
                        return nzb_file

    ############################################################################
    def _get_rar_files(self, nzb_files):
        rar_files      = []
        first_rar_file = self._get_first_rar_file(nzb_files)

        for nzb_file in nzb_files:
            if RE_RAR.search(nzb_file.name) or RE_RXX.search(nzb_file.name) or RE_XXX.search(nzb_file.name):
                if nzb_file != first_rar_file:
                    rar_files.append(nzb_file)
        
        rar_files.sort(key=lambda rar_file: rar_file.name)
        if first_rar_file:
            rar_files.insert(0, first_rar_file)
        return rar_files

    ############################################################################
    def _get_remaining_files(self, nzb_files, current_files):
        files = []
        for nzb_file in nzb_files:
            if nzb_file not in current_files:
                files.append(nzb_file)
        return files

    ############################################################################
    def _get_file_crc32(self, nzb_file):
        if os.path.isfile(nzb_file.path):
            return '%X' % (zlib.crc32(open(nzb_file.path, 'rb').read()) & 0xFFFFFFFF)

    ############################################################################
    def _parse_sfv_file(self, sfv_file_name, nzb_files):
        RE_SFV_LINE = re.compile('(\S+)\s+(\w+)')

        complete_files   = []
        incomplete_files = []

        sfv_data = {}
        with open(sfv_file_name, 'r') as sfv_file:
            for sfv_line in sfv_file:
                sfv_data[RE_SFV_LINE.search(sfv_line).group(1).lower()] = RE_SFV_LINE.search(sfv_line).group(2).lower()

        for nzb_file in nzb_files:
            if not hasattr(nzb_file, 'crc32'):
                nzb_file.crc32 = self._get_file_crc32(nzb_file)
                if nzb_file.crc32 == None:
                    incomplete_files.append(nzb_file)
                    continue

            if nzb_file.name.lower() in sfv_data:
                if nzb_file.crc32.lower() == sfv_data[nzb_file.name.lower()].lower():
                    complete_files.append(nzb_file)
                else:
                    incomplete_files.append(nzb_file)

        return (complete_files, incomplete_files)

    ############################################################################
    def _write_nzb_file(self, nzb_file, nzb_segments):
        if not os.path.isdir(self.nzb_dir):
            os.makedirs(self.nzb_dir)

        with open(os.path.join(nzb_file.path), 'w+', 0) as nzb_file:
            for nzb_segment in nzb_segments:
                nzb_file.write(nzb_segment)

