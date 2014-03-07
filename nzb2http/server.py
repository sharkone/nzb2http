################################################################################
import cherrypy
import datetime
import downloader
import json
import filewrapper
import mimetypes
import os
import rarfile
import re
import time

from cherrypy.lib.static import serve_fileobj

################################################################################
class ConnectionCounterTool(cherrypy.Tool):
    ############################################################################
    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_handler', self._before_handler)
        self.connection_count     = 0
        self.last_connection_time = datetime.datetime.now()

    ############################################################################
    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.serving.request.hooks.attach('on_end_request', self._on_end_request)

    ############################################################################
    def _before_handler(self):
        self.connection_count = self.connection_count + 1

    ############################################################################
    def _on_end_request(self):
        self.connection_count = self.connection_count - 1
        if not self.connection_count:
            self.last_connection_time = datetime.datetime.now()

cherrypy.tools.connectioncounter = ConnectionCounterTool()

################################################################################
class AutoShutdownMonitor(cherrypy.process.plugins.Monitor):
    ############################################################################
    def __init__(self, bus, timeout):
        cherrypy.process.plugins.Monitor.__init__(self, bus, self._check_for_timeout, frequency=5)
        self.timeout = timeout

    ############################################################################
    def _check_for_timeout(self):
        if not cherrypy.tools.connectioncounter.connection_count and (datetime.datetime.now() - cherrypy.tools.connectioncounter.last_connection_time) >= datetime.timedelta(seconds=self.timeout):
            cherrypy.engine.exit()

################################################################################
class NzbDownloaderPlugin(cherrypy.process.plugins.SimplePlugin):
    ############################################################################
    def __init__(self, bus, nntp_credentials, download_dir, nzb_name, nzb_content):
        cherrypy.process.plugins.SimplePlugin.__init__(self, bus)
        self.downloader = downloader.Downloader(nntp_credentials, download_dir, nzb_name, nzb_content)

    ############################################################################
    def start(self):
        self.downloader.start()

    ############################################################################
    def stop(self):
        self.downloader.stop()

################################################################################
class Server:
    ############################################################################
    def __init__(self, port, nntp_credentials, download_dir, timeout, nzb_name, nzb_content):
        self.port = port
        
        cherrypy.engine.autoshutdown = AutoShutdownMonitor(cherrypy.engine, timeout)
        cherrypy.engine.autoshutdown.subscribe()
        
        cherrypy.engine.nzbdownloader = NzbDownloaderPlugin(cherrypy.engine, nntp_credentials, download_dir, nzb_name, nzb_content)
        cherrypy.engine.nzbdownloader.subscribe()

    ############################################################################
    def run(self):
        cherrypy.config.update({'server.socket_host':'0.0.0.0'})
        cherrypy.config.update({'server.socket_port':self.port})

        cherrypy.quickstart(ServerRoot())

################################################################################
class ServerRoot:
    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.connectioncounter()
    def index(self):
        return json.dumps(cherrypy.engine.nzbdownloader.downloader.extractor.files)

    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.connectioncounter()
    def download(self):
        video_file = self._get_first_video_file(cherrypy.engine.nzbdownloader.downloader.extractor.files)
        if not video_file:
            return 'Not ready!'

        return serve_fileobj(filewrapper.FileWrapper(video_file['path'], video_file['size']), content_type='application/x-download', content_length=video_file['size'], disposition='attachment', name=os.path.basename(video_file['path']))

    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.connectioncounter()
    def video(self):
        video_file = self._get_first_video_file(cherrypy.engine.nzbdownloader.downloader.extractor.files)
        if not video_file:
            return 'Not ready!'

        content_type = mimetypes.types_map.get(os.path.splitext(video_file['path']), None)
        
        if not content_type:
            if video_file['path'].endswith('.mkv'):
                content_type = 'video/x-matroska'
            elif video_file['path'].endswith('.mp4'):
                content_type = 'video/mp4'

        return serve_fileobj(filewrapper.FileWrapper(video_file['path'], video_file['size']), content_type=content_type, content_length=video_file['size'], name=os.path.basename(video_file['path']))

    ############################################################################
    @cherrypy.expose
    def shutdown(self):
        cherrypy.engine.exit()
        return 'OK'

    ############################################################################
    def _get_first_video_file(self, files):
        video_file = None
        for file in cherrypy.engine.nzbdownloader.downloader.extractor.files:
            if file['path'].endswith('.mkv') or file['path'].endswith('.mp4'):
                return file
