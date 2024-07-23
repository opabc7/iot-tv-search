#!/usr/bin/env python3

import faulthandler
import os
from vod.handler import VodHandler
from lib.rocksclient import RocksClient

class AlbumHeat(VodHandler):

    def __init__(self, work_dir):
        self.task = os.environ['vod_task'] = 'album_heat'

        VodHandler.__init__(self, work_dir, 'sid', None)

        self.rocksclient = RocksClient(self.rocksdb_path, 'rw')

    def init_config_task(self):
        task_config = VodHandler.init_config_task(self)

        # config:task:rocksdb
        self.rocksdb_path = os.path.join(self.rocksdb_config['root'], self.rocksdb_config[self.task])

    def write_plus(self, _id, title, body_plus, doc_plus):
        self.rocksclient.put(_id, body_plus)
        self.logger.info('rocks put - %s', body_plus)

if __name__ == '__main__':
    faulthandler.enable()

    AlbumHeat(os.path.dirname(__file__)).start()
