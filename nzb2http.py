################################################################################
import argparse
import nzb2http
import os
import re
import urllib

from nzb2http import server

################################################################################
def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('server', help='Usenet server (username:password@host:port)')
    arg_parser.add_argument('nzb_path', help='NZB file or URL')
    arg_parser.add_argument('-p', '--http-port', default=8080, help='Port used for HTTP server')
    arg_parser.add_argument('-d', '--download-dir', default='.', help='Directory to use for downloading')
    arg_parser.add_argument('-s', '--ssl', action='store_true', help='Use SSL connection')
    arg_parser.add_argument('-m', '--connections', default=1, help='Max concurrent connections')
    args = arg_parser.parse_args()

    RE_HOST = re.compile('(.+):(.+)@(.+):(\d+)')

    nntp_credentials = {    
                            'host':             RE_HOST.search(args.server).group(3),
                            'port':             int(RE_HOST.search(args.server).group(4)),
                            'username':         RE_HOST.search(args.server).group(1),
                            'password':         RE_HOST.search(args.server).group(2),
                            'use_ssl':          args.ssl,
                            'max_connections':  int(args.connections)
                       }

    if os.path.isfile(args.nzb_path):
        nzb_name = os.path.basename(args.nzb_path)
        with open(args.nzb_path, 'r') as nzb:
            nzb_content = nzb.read()
    else:
        filename, headers = urllib.urlretrieve(args.nzb_path)
        nzb_name          = re.search('.+filename=(.+)$', headers['Content-Disposition']).group(1)  
        with open(filename, 'r') as nzb:
            nzb_content = nzb.read()        

    server = nzb2http.server.Server(int(args.http_port), nntp_credentials, args.download_dir, nzb_name, nzb_content)
    server.run()

################################################################################
if __name__ == '__main__':
    main()
