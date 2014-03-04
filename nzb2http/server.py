################################################################################
import cherrypy
import datetime
import downloader
import json
import mimetypes
import os
import rarfile
import re
import signal
import time

from cherrypy.lib.static import serve_fileobj

################################################################################
RAR_FILE = None

################################################################################
class AutoShutdown(cherrypy.Tool):
    def __init__(self, timeout):
        cherrypy.Tool.__init__(self, 'before_handler', self._before_handler)
        self.timeout              = timeout
        self.connection_count     = 0
        self.last_connection_time = datetime.datetime.now()

    def has_timeouted(self):
        return not self.connection_count and (datetime.datetime.now() - self.last_connection_time) >= datetime.timedelta(seconds=self.timeout)

    def _setup(self):
        cherrypy.serving.request.hooks.attach('on_end_request', self._on_end_request)

    def _before_handler(self):
        self.connection_count = self.connection_count + 1

    def _on_end_request(self):
        self.connection_count = self.connection_count - 1
        if not self.connection_count:
            sell.connection_count = datetime.datetime.now()

cherrypy.tools.autoshutdown = AutoShutdown(30)

################################################################################
class Server:
    ############################################################################
    def __init__(self, port, nntp_credentials, download_dir, nzb_name, nzb_content):
        self.port       = port
        self.downloader = downloader.Downloader(nntp_credentials, download_dir, nzb_name, nzb_content)

    ############################################################################
    def start(self):
        cherrypy.config.update({'engine.autoreload.on':False})
        cherrypy.config.update({'server.socket_host':'0.0.0.0'})
        cherrypy.config.update({'server.socket_port':self.port})
        cherrypy.tree.mount(ServerRoot(self.downloader))

        cherrypy.engine.start()
        self.downloader.start()

    ############################################################################
    def stop(self):
        self.downloader.stop()
        cherrypy.engine.exit()

    ############################################################################
    def has_timeouted(self):
        return cherrypy.tools.autoshutdown.has_timeouted()

################################################################################
class ServerRoot:
    ############################################################################
    def __init__(self, downloader):
        self.downloader = downloader

    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.autoshutdown()
    def index(self):
        rar_file = self._get_rar_file()

        result =  {
                      'nzb':    os.path.basename(self.downloader.nzb_name),
                      'ready':  rar_file.is_ready if rar_file else None
                  }

        return json.dumps(result)

    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.autoshutdown()
    def download(self):
        rar_file = self._get_rar_file()
        if not rar_file or not rar_file.is_ready:
            return 'Data is not ready yet!'

        return serve_fileobj(rar_file, content_type='application/x-download', content_length=rar_file.content_file_size, last_modified=time.time(), disposition='attachment', name=rar_file.content_file_name)

    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.autoshutdown()
    def video(self):
        rar_file = self._get_rar_file()
        if not rar_file or not rar_file.is_ready:
            return 'Data is not ready yet!'

        content_type = mimetypes.types_map.get(os.path.splitext(rar_file.content_file_name), None)
        if not content_type:
            if rar_file.content_file_name.endswith('.mkv'):
                content_type = 'video/x-matroska'
            elif rar_file.content_file_name.endswith('.mp4'):
                content_type = 'video/mp4'
    
        return serve_fileobj(rar_file, content_type=content_type, content_length=rar_file.content_file_size, last_modified=time.time(), name=rar_file.content_file_name)

    ############################################################################
    @cherrypy.expose
    def shutdown(self):
        os.kill(os.getpid(), signal.SIGINT)
        return 'OK'

    ############################################################################
    def _get_rar_path(self):
        RE_PART_XX = re.compile('.part\d+.rar$')
        RE_PART_01 = re.compile('.part01.rar$')
        RE_001     = re.compile('.001$')

        rar_filenames = [os.path.join(dirpath, f) for dirpath, dirnames, files in os.walk(self.downloader.nzb_dir) for f in files if ((f.endswith('.rar') and (not 'subs' in f) and (not RE_PART_XX.search(f) or RE_PART_01.search(f))) or RE_001.search(f))]
        if rar_filenames:
            return rar_filenames[0]

    ############################################################################
    def _get_rar_file(self):
        global RAR_FILE

        rar_path = self._get_rar_path()
        if not RAR_FILE and rar_path != None:
            RAR_FILE = rarfile.RarFile(rar_path)

        return RAR_FILE
