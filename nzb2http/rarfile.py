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
	def __init__(self, rar_path):
		self.rar_path          = rar_path
		self.is_ready          = False
		self.content_file_name = None
		self.content_file_size = None
		self.thread            = None
		self.eof_reached       = False
		self._start_thread()

	def read(self, n=-1):
		if n == -1:
			return self.readall()

		if self.eof_reached:
			return ""

		result     = ""
		read_count = 0
		eof_found  = False
		while read_count != n:
			if not self.buffer:
				self.buffer = self.queue.get()
				self.queue.task_done()
				if self.buffer == ['R', 'A', 'R', 'E', 'O', 'F']:
					self.eof_reached = True
					self.buffer      = []
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

	def readall(self):
		return self.read(self.get_content_file_size())

	def seek(self, offset, whence=io.SEEK_SET):
		if whence == io.SEEK_SET:
			new_position = offset
		elif whence == io.SEEK_CUR:
			new_position = self.position + offset
		elif whence == io.SEEK_END:
			new_position = self.thread.content_file_size + offset

		if self._can_seek(new_position):
			if new_position > self.position:
				self.read(new_position - self.position)
			elif new_position < self.position:
				self._start_thread()
				self.read(offset)
			print '[nzb2http][rarfile] Seeking @ {0} successful'.format(new_position)
		else:
			print '[nzb2http][rarfile] Seeking @ {0} failed'.format(new_position)

		if self.position < self.content_file_size:
			self.eof_reached = False

		return self.position

	def close(self):
		self.thread.stop_requested = True
		while not self.queue.empty():
			self.queue.get()
			self.queue.task_done()
		self.thread.join()

	def _start_thread(self):
		self.queue         = Queue.Queue(10)
		self.buffer        = []
		self.position      = 0
		self.thread        = RarFileWorkerThread(self)
		self.thread.daemon = True
		self.thread.start()

	def _can_seek(self, absolute_offset):
		if self.position >= absolute_offset:
			return True

		archive_data   = unrarlib.RAROpenArchiveDataEx(self.rar_path, mode=unrarlib.constants.RAR_OM_LIST_INCSPLIT)
		archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))	

		current_position = 0

		callback = unrarlib.UNRARCALLBACK(self._can_seek_callback)
		unrarlib.RARSetCallback(archive_handle, callback, 0)

		try:
			header_result, header_data = self._read_header(archive_handle) 
			while current_position < absolute_offset:
				current_position = current_position + header_data.PackSize
				unrarlib.RARProcessFileW(archive_handle, unrarlib.constants.RAR_SKIP, None, None)
				header_result, header_data = self._read_header(archive_handle)
		except unrarlib.UnrarException:
			pass

		unrarlib.RARCloseArchive(archive_handle)

		return current_position >= absolute_offset

	def _can_seek_callback(self, msg, UserData, P1, P2):
		if msg == unrar.constants.UCM_CHANGEVOLUME:
			if P2 == 0:
				return -1
		return 1

	def _read_header(self, archive_handle):
		header_data   = unrarlib.RARHeaderDataEx()
		header_result = unrarlib.RARReadHeaderEx(archive_handle, ctypes.byref(header_data))
		return (header_result, header_data)

################################################################################
class RarFileWorkerThread(threading.Thread):
	def __init__(self, rarfile):
		threading.Thread.__init__(self)
		self.rarfile = rarfile


	def run(self):
		self.stop_requested = False

		while not self.stop_requested:
			try:
				archive_data   = unrarlib.RAROpenArchiveDataEx(self.rarfile.rar_path, mode=unrarlib.constants.RAR_OM_LIST)
				archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))

				header_result, header_data = self.rarfile._read_header(archive_handle)

				self.rarfile.is_ready          = True
				self.rarfile.content_file_name = header_data.FileName
				self.rarfile.content_file_size = header_data.UnpSize

				unrarlib.RARCloseArchive(archive_handle)
				break
			except unrarlib.UnrarException:
				pass 

		if not self.stop_requested:
			archive_data   = unrarlib.RAROpenArchiveDataEx(self.rarfile.rar_path, mode=unrarlib.constants.RAR_OM_EXTRACT)
			archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))

			callback = unrarlib.UNRARCALLBACK(self._extract_callback)
			unrarlib.RARSetCallback(archive_handle, callback, 0)

			sys.stdout.write('[nzb2http][rarfile] Extracting from {0}\n'.format(self.rarfile.rar_path))

			try:
				header_result, header_data = self.rarfile._read_header(archive_handle)
				while not self.stop_requested and header_result is unrarlib.constants.SUCCESS:
					unrarlib.RARProcessFileW(archive_handle, unrarlib.constants.RAR_TEST, None, None)
					header_result, header_data = self.rarfile._read_header(archive_handle)
			except unrarlib.UnrarException:
				pass 

			unrarlib.RARCloseArchive(archive_handle)
			self.rarfile.queue.put(['R', 'A', 'R', 'E', 'O', 'F'])

	def _extract_callback(self, msg, UserData, P1, P2):
		if self.stop_requested:
			return -1

		if msg == unrar.constants.UCM_PROCESSDATA:
			data = (ctypes.c_char*P2).from_address(P1).raw
			self.rarfile.queue.put(data)
		elif msg == unrar.constants.UCM_CHANGEVOLUME:
			next_volume = ctypes.c_char_p(P1).value
			if P2 == 0:
				sys.stdout.write('[nzb2http][rarfile] Waiting for {0}\n'.format(next_volume))
				time.sleep(1)
			elif P2 == 1:
				sys.stdout.write('[nzb2http][rarfile] Extracting from {0}\n'.format(next_volume))
		return 1
