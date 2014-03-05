################################################################################
import ctypes
import io
import unrar

from unrar import unrarlib

################################################################################
class RarFile(io.RawIOBase):
	def __init__(self, path):
		self.path           = path
		self._open()

	def close(self):
		pass
		if self.archive_handle:
			unrarlib.RARCloseArchive(self.archive_handle)

	def _open(self):
		# Initial open
		archive_data   = unrarlib.RAROpenArchiveDataEx(self.path, mode=unrarlib.constants.RAR_OM_LIST_INCSPLIT)
		archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))	
		header_result, header_data = self._read_header(archive_handle)
		unrarlib.RARCloseArchive(archive_handle)

		archive_data   = unrarlib.RAROpenArchiveDataEx(self.path, mode=unrarlib.constants.RAR_OM_EXTRACT)
		self.archive_handle = unrarlib.RAROpenArchiveEx(ctypes.byref(archive_data))

		#self.archive_data = archive_data
		#self.archive_handle = archive_handle

	def _read_header(self, archive_handle):
		header_data   = unrarlib.RARHeaderDataEx()
		header_result = unrarlib.RARReadHeaderEx(archive_handle, ctypes.byref(header_data))
		return (header_result, header_data)
