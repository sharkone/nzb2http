################################################################################
import datetime
import nntp
import os
import pynzb
import re
import sys
import threading
import time
import yenc
import Queue

################################################################################
class Downloader(threading.Thread):
    ############################################################################
    def __init__(self, nntp_credentials, nzb_path, download_dir):
        threading.Thread.__init__(self)
        self.nntp_credentials = nntp_credentials
        self.nzb_path         = nzb_path
        self.nzb_dir          = os.path.join(download_dir, self.nzb_path[:-4])

        sys.stdout.write('[nzb2http][downloader] Downloading {0}\n'.format(self.nzb_path))
        with open(self.nzb_path, 'r') as nzb_file:
            self.nzb_files = pynzb.nzb_parser.parse(nzb_file.read())
        #sys.stdout.write('[nzb2http][downloader] {0} files found\n'.format(len(self.nzb_files)))

        self.nzb_files = self._sort_files(self.nzb_files)
        #sys.stdout.write('[nzb2http][downloader] {0} files will be downloaded\n'.format(len(self.nzb_files)))
        #for nzb_file in self.nzb_files:
        #    sys.stdout.write('[nzb2http][downloader] - {0}\n'.format(self._get_nzb_file_name(nzb_file)))

    ############################################################################
    def run(self):
        sys.stdout.write('[nzb2http][downloader] Started\n')
        self.segment_queue  = self._fill_segment_queue(self.nzb_files)
        self.write_queue    = Queue.Queue()
        self.thread_pool    = self._create_thread_pool(self.nntp_credentials, self.segment_queue, self.write_queue)
        self.stop_requested = False

        downloaded_files = {}
        for nzb_file in self.nzb_files:
            downloaded_files[self._get_nzb_file_name(nzb_file)] = [None] * len(nzb_file.segments)

        while (not self.segment_queue.empty() or downloaded_files) and not self.stop_requested:
            try:
                segment_number, segment_count, content_file_name, content_file_offset, content_file_data = self.write_queue.get(True, 1)
                downloaded_files[content_file_name][segment_number - 1] = (segment_number, segment_count, content_file_name, content_file_offset, content_file_data)
                self.write_queue.task_done()
            except Queue.Empty:
                pass
            
            if not self.stop_requested:
                to_remove = [] 
                for content_file_name, segment_list in downloaded_files.iteritems():
                    for i, segment in enumerate(segment_list):
                        if segment != None:
                            if segment != 'DONE':
                                sys.stdout.write('[nzb2http][downloader] Downloaded %s -- (%02d/%02d)\n' % (segment[2], segment[0], segment[1]))
                                self._write_content_data(segment[2], segment[3], segment[4])
                                downloaded_files[content_file_name][i] = 'DONE'
                        else:
                            break 

                    all_done = True
                    for segment in segment_list:
                        if segment != 'DONE':
                            all_done = False
                            break
                    if all_done:
                        to_remove.append(content_file_name)

                for item in to_remove:
                    del downloaded_files[item]

        self._stop_thread_pool(self.thread_pool)
        sys.stdout.write('[nzb2http][downloader] Stopped\n')

    ############################################################################
    def stop(self):
        sys.stdout.write('[nzb2http][downloader] Stopping\n')
        self.stop_requested = True
        self.join()

    ############################################################################
    def _sort_files(self, nzb_files):
        sorted_files = []
        sorted_files = sorted_files + self._get_files(nzb_files, '.sfv')
        sorted_files = sorted_files + self._get_files(nzb_files, '.srr')
        sorted_files = sorted_files + self._get_rar_files(nzb_files)
        sorted_files = sorted_files + self._get_files(nzb_files, '.par2')
        sorted_files = sorted_files + self._get_remaining_files(nzb_files, sorted_files)
        return sorted_files

    ############################################################################
    def _get_files(self, nzb_files, pattern):
        files = []
        for nzb_file in nzb_files:
            if self._get_nzb_file_name(nzb_file).endswith(pattern):
                files.append(nzb_file)
        return files

    ############################################################################
    def _get_rar_files(self, nzb_files):
        RE_RAR     = re.compile('\.rar$')
        RE_RXX     = re.compile('\.r\d+$')
        RE_XXX     = re.compile('\.\d\d\d$')
        RE_PART_XX = re.compile('.part\d+.rar$')
        RE_PART_01 = re.compile('.part01.rar$')
        RE_001     = re.compile('.001$')

        rar_files    = []
        first_volume = None
        for nzb_file in nzb_files:
            nzb_file_name = self._get_nzb_file_name(nzb_file)
            if RE_RAR.search(nzb_file_name) or RE_RXX.search(nzb_file_name) or RE_XXX.search(nzb_file_name):
                if not first_volume:
                    if (RE_RAR.search(nzb_file_name) or RE_PART_01.search(nzb_file_name) or RE_001.search(nzb_file_name)) and not 'subs' in nzb_file_name:
                        first_volume = nzb_file
                        continue
                rar_files.append(nzb_file)
        rar_files.insert(0, first_volume)
        return rar_files

    ############################################################################
    def _get_remaining_files(self, nzb_files, current_files):
        files = []
        for nzb_file in nzb_files:
            if nzb_file not in current_files:
                files.append(nzb_file)
        return files

    ############################################################################
    def _get_nzb_file_name(self, nzb_file):
        RE_FILENAME = re.compile('\"(.+)\"')
        return RE_FILENAME.search(nzb_file.subject).group(1)

    ############################################################################
    def _fill_segment_queue(self, nzb_files):
        segment_queue = Queue.Queue()
        for nzb_file in nzb_files:
            for nzb_file_segment in nzb_file.segments:
                segment_queue.put((self._get_nzb_file_name(nzb_file), nzb_file_segment, len(nzb_file.segments)))
        return segment_queue
    
    ############################################################################
    def _create_thread_pool(self, nntp_credentials, segment_queue, write_queue):
        threads = []
        for i in range(nntp_credentials['max_connections']):
            t = DownloaderWorker(i, nntp_credentials, segment_queue, write_queue)
            t.start()
            threads.append(t)
        return threads

    ############################################################################
    def _stop_thread_pool(self, thread_pool):
        for thread in thread_pool:
            thread.stop()

        for thread in thread_pool:
            thread.join()
    
    ############################################################################
    def _write_content_data(self, content_file_name, content_file_offset, content_file_data):
        if not os.path.isdir(self.nzb_dir):
            os.mkdir(self.nzb_dir)

        if not os.path.isfile(os.path.join(self.nzb_dir, content_file_name)):
            open(os.path.join(self.nzb_dir, content_file_name), 'a').close()

        with open(os.path.join(self.nzb_dir, content_file_name), 'r+b', 0) as content_file:
            content_file.seek(content_file_offset, 0)
            content_file.write(content_file_data)

################################################################################
class DownloaderWorker(threading.Thread):
    ############################################################################
    def __init__(self, index, nntp_credentials, segment_queue, write_queue):
        threading.Thread.__init__(self)
        self.index            = index
        self.nntp_credentials = nntp_credentials
        self.segment_queue    = segment_queue
        self.write_queue      = write_queue

    ############################################################################
    def run(self):
        #sys.stdout.write('[nzb2http][downloader][%02d] Started\n' % (self.index))
        self.connection     = nntp.NNTP(self.nntp_credentials['host'], self.nntp_credentials['port'], self.nntp_credentials['username'], self.nntp_credentials['password'], self.nntp_credentials['use_ssl'])
        self.stop_requested = False

        while not self.stop_requested:
            try:
                file_name, segment, segment_count = self.segment_queue.get_nowait()
            except Queue.Empty:
                break

            #sys.stdout.write('[nzb2http][downloader][%02d] Downloading %s -- (%02d/%02d) %s\n' % (self.index, file_name, segment.number, segment_count, segment.message_id))
            response, number, message_id, text_lines = self.connection.article('<' + segment.message_id + '>')

            content_file_name, content_file_offset, content_file_data = self._yenc_decode(text_lines)
            self.write_queue.put((segment.number, segment_count, content_file_name, content_file_offset, content_file_data))
            #sys.stdout.write('[nzb2http][downloader][%02d] Downloaded %s -- (%02d/%02d) %s\n' % (self.index, content_file_name, segment.number, segment_count, segment.message_id))
            
            self.segment_queue.task_done()

        self.connection.quit()
        #sys.stdout.write('[nzb2http][downloader][%02d] Stopped\n' % (self.index))

    ############################################################################
    def stop(self):
        #sys.stdout.write('[nzb2http][downloader][%02d] Stopping\n' % (self.index))
        self.stop_requested = True

    ############################################################################
    def _yenc_decode(self, text_lines):
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

        return (content_file_name, content_file_offset, yenc_decoder.getDecoded())
