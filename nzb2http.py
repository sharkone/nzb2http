################################################################################
import argparse
import cherrypy
import nzb2http
import pynzb
import re
import signal
import sys
import time

from nzb2http import downloader
from nzb2http import server

################################################################################
RUNNING = False

################################################################################
def signal_handler(signal, frame):
    global RUNNING
    sys.stdout.write('[nzb2http] SIGINT signal caught\n')
    RUNNING = False

################################################################################
def main():
    global RUNNING

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('server', help='Usenet server (username:password@host:port')
    arg_parser.add_argument('nzb_path', help='NZB file to use')
    arg_parser.add_argument('-d', '--download-dir', default='.', help='Directory to use for downloading')
    arg_parser.add_argument('-s', '--ssl', action='store_true', help='Use SSL connection')
    arg_parser.add_argument('-m', '--connections', default=1, help='Max concurrent connections')
    args = arg_parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)

    RE_HOST = re.compile('(.+):(.+)@(.+):(\d+)')

    nntp_credentials = {    
                            'host':             RE_HOST.search(args.server).group(3),
                            'port':             int(RE_HOST.search(args.server).group(4)),
                            'username':         RE_HOST.search(args.server).group(1),
                            'password':         RE_HOST.search(args.server).group(2),
                            'use_ssl':          args.ssl,
                            'max_connections':  int(args.connections)
                       }

    downloader = nzb2http.downloader.Downloader(nntp_credentials, args.nzb_path, args.download_dir)
    downloader.start()

    server = nzb2http.server.Server(args.nzb_path)
    server.start()

    RUNNING = True
    while RUNNING and not server.has_timeouted():
        time.sleep(1)

    downloader.stop()
    server.stop()

################################################################################
if __name__ == '__main__':
    main()
