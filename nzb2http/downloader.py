################################################################################
import multiprocessing
import nntp
import os
import pynzb
import re
import sys
import threading
import yenc

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
            nzb_file.name = RE_NZB_FILE_NAME.search(nzb_file.subject).group(1)
        self.nzb_files = self._sort_files(self.nzb_files)

        sys.stdout.write('[nzb2http][downloader] {0} files will be downloaded in order:\n'.format(len(self.nzb_files)))
        for nzb_file in self.nzb_files:
           sys.stdout.write('[nzb2http][downloader] - {0}\n'.format(nzb_file.name))

        self.pool = multiprocessing.pool.ThreadPool(self.nntp_credentials['max_connections'], _init_worker, (self.nntp_credentials,))

    ############################################################################
    def run(self):
        sys.stdout.write('[nzb2http][downloader] Started\n')
        self.stop_requested = False

        for nzb_file in self.nzb_files:
            map_result_async = self.pool.map_async(_run_worker, nzb_file.segments)
            while not self.stop_requested:
                try:
                    map_result = map_result_async.get(1)
                    sys.stdout.write('[nzb2http][downloader] Downloaded {0}\n'.format(os.path.join(self.nzb_dir, nzb_file.name)))
                    self._write_nzb_file(nzb_file.name, map_result)
                    break
                except multiprocessing.TimeoutError:
                    pass

        self.pool.terminate()
        self.pool.join()

        sys.stdout.write('[nzb2http][downloader] Stopped\n')

    ############################################################################
    def stop(self):
        sys.stdout.write('[nzb2http][downloader] Stopping\n')
        self.stop_requested = True
        self.join()

    ############################################################################
    def get_first_rar_path(self):
        return os.path.join(self.nzb_dir, self._get_first_rar_file(self.nzb_files).name)

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
    def _write_nzb_file(self, nzb_file_name, nzb_segments):
        if not os.path.isdir(self.nzb_dir):
            os.makedirs(self.nzb_dir)

        with open(os.path.join(self.nzb_dir, nzb_file_name), 'w+', 0) as nzb_file:
            for nzb_segment in nzb_segments:
                nzb_file.write(nzb_segment)
