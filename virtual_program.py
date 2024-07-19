#!/usr/bin/env python3

import faulthandler
import os
from lib.vod  import Vod
from lib.rocksclient import RocksClient

class VirtualProgram(Vod):

    def __init__(self, work_dir):
       os.environ['vod_task'] = 'virtual_program'

       Vod.__init__(self, work_dir, 'virtualSid', 'title')

       self.rocksclient = RocksClient(self.rocksdb_path, 'rw')
       self.rocksclient_back = RocksClient(self.rocksdb_path_back, 'rw')

    def init_config_task(self):
        task_config = Vod.init_config_task(self)

        # config:task:rocksdb
        self.rocksdb_path = task_config['rocksdb']['path']
        self.rocksdb_path_back = task_config['rocksdb']['path_back']

    def write_plus(self, _id, title, body_plus, doc_plus):
        self.rocksclient.put(_id, body_plus)
        self.logger.info('rocks put(vp) - %s - %s', _id, body_plus)

        if 'virtualProgramRelList' in doc_plus and len(doc_plus['virtualProgramRelList']):
            for vlist in doc_plus['virtualProgramRelList']:
                if 'sid' in vlist:
                    self.rocksclient_back.put(vlist['sid'], body_plus)
                    self.logger.info('rocks put(album) - %s - %s', vlist['sid'], body_plus)

if __name__ == '__main__':
    faulthandler.enable()

    VirtualProgram(os.path.dirname(__file__)).start()
