################################################################################
import cherrypy
import datetime
import json
import mimetypes
import os
import rarfile
import re
import signal
import time

from cherrypy.lib.static import serve_fileobj

################################################################################
RAR_FILE             = None
CONNECTION_COUNT     = 0
LAST_CONNECTION_TIME = datetime.datetime.now()

################################################################################
def count_connection_before_handler():
    global CONNECTION_COUNT
    CONNECTION_COUNT = CONNECTION_COUNT + 1

################################################################################
def count_connection_on_end_request():
    global CONNECTION_COUNT, LAST_CONNECTION_TIME
    CONNECTION_COUNT = CONNECTION_COUNT - 1

    if not CONNECTION_COUNT:
        LAST_CONNECTION_TIME = datetime.datetime.now()

################################################################################
cherrypy.tools.count_connection_before_handler = cherrypy.Tool('before_handler', count_connection_before_handler)
cherrypy.tools.count_connection_on_end_request = cherrypy.Tool('on_end_request', count_connection_on_end_request)

################################################################################
class Server:
    ############################################################################
    def __init__(self, port, nzb_path):
        self.port     = port
        self.nzb_path = nzb_path

    ############################################################################
    def start(self):
        cherrypy.config.update({'engine.autoreload.on':False})
        cherrypy.config.update({'server.socket_host':'0.0.0.0'})
        cherrypy.config.update({'server.socket_port':self.port})
        cherrypy.tree.mount(ServerRoot(self.nzb_path))

        cherrypy.engine.start()

    ############################################################################
    def stop(self):
        cherrypy.engine.exit()

    ############################################################################
    def has_timeouted(self):
        global CONNECTION_COUNT
        return not CONNECTION_COUNT and (datetime.datetime.now() - LAST_CONNECTION_TIME) >= datetime.timedelta(seconds=30)

################################################################################
class ServerRoot:
    ############################################################################
    def __init__(self, nzb_path):
        self.nzb_path = nzb_path
        self.nzb_dir  = self.nzb_path[:-4]

    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.count_connection_before_handler()
    @cherrypy.tools.count_connection_on_end_request()
    def index(self):
        rar_file = self._get_rar_file()

        result =  {
                      'nzb_file':     os.path.basename(self.nzb_path),
                      'ready':        rar_file.is_ready if rar_file else None
                  }

        return json.dumps(result)

    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.count_connection_before_handler()
    @cherrypy.tools.count_connection_on_end_request()
    def download(self):
        rar_file = self._get_rar_file()
        if not rar_file or not rar_file.is_ready:
            return 'Data is not ready yet!'

        return serve_fileobj(rar_file, content_type='application/x-download', content_length=rar_file.content_file_size, last_modified=time.time(), disposition='attachment', name=rar_file.content_file_name)

    ############################################################################
    @cherrypy.expose
    @cherrypy.tools.count_connection_before_handler()
    @cherrypy.tools.count_connection_on_end_request()
    def video(self):
        rar_file = self._get_rar_file()
        if not rar_file or not rar_file.is_ready:
            return 'Data is not ready yet!'

        print '-----> {0} - {1}'.format(rar_file.content_file_name, rar_file.content_file_size)
        content_type = mimetypes.types_map.get(os.path.splitext(rar_file.content_file_name), None)
        if not content_type:
            if rar_file.content_file_name.endswith('.mkv'):
                content_type = 'video/x-matroska'
            elif rar_file.content_file_name.endswith('.mp4'):
                content_type = 'video/mp4'
    
        return serve_fileobj(rar_file, content_type=content_type, content_length=rar_file.content_file_size, last_modified=time.time(), name=rar_file.content_file_name, debug=True)

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

        rar_filenames = [os.path.join(dirpath, f) for dirpath, dirnames, files in os.walk(self.nzb_dir ) for f in files if ((f.endswith('.rar') and (not 'subs' in f) and (not RE_PART_XX.search(f) or RE_PART_01.search(f))) or RE_001.search(f))]
        if rar_filenames:
            return rar_filenames[0]

    ############################################################################
    def _get_rar_file(self):
        global RAR_FILE

        rar_path = self._get_rar_path()
        if not RAR_FILE and rar_path != None:
            RAR_FILE = rarfile.RarFile(rar_path)

        return RAR_FILE
