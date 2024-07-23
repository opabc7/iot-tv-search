#!/usr/bin/env python3

import faulthandler
import os
from vod.handler import VodHandler
from lib.rocksclient import RocksClient

class VirtualProgram(VodHandler):

    def __init__(self, work_dir):
        self.task = os.environ['vod_task'] = 'virtual_program'

        VodHandler.__init__(self, work_dir, 'virtualSid', 'title')

        self.rocksclient = RocksClient(self.rocksdb_path, 'rw')
        self.rocksclient_album = RocksClient(self.rocksdb_path_album, 'rw')

    def init_config_task(self):
        task_config = VodHandler.init_config_task(self)

        # config:task:rocksdb
        self.rocksdb_path = os.path.join(self.rocksdb_config['root'], self.rocksdb_config[self.task])
        self.rocksdb_path_album = os.path.join(self.rocksdb_config['root'], self.rocksdb_config[task_config['rocksdb']['album']])

    def write_plus(self, _id, title, body_plus, doc_plus):
        self.rocksclient.put(_id, body_plus)
        self.logger.info('rocks put(vp) - %s - %s', _id, body_plus)

        if 'virtualProgramRelList' in doc_plus and len(doc_plus['virtualProgramRelList']):
            for vlist in doc_plus['virtualProgramRelList']:
                if 'sid' in vlist:
                    self.rocksclient_album.put(vlist['sid'], body_plus)
                    self.logger.info('rocks put(album) - %s - %s', vlist['sid'], body_plus)

if __name__ == '__main__':
    faulthandler.enable()

    VirtualProgram(os.path.dirname(__file__)).start()
