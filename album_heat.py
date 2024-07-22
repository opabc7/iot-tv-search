#!/usr/bin/env python3

import faulthandler
import os
from vod.base import Vod
from lib.rocksclient import RocksClient

class AlbumHeat(Vod):

    def __init__(self, work_dir):
        self.task = os.environ['vod_task'] = 'album_heat'

        Vod.__init__(self, work_dir, 'sid', None)

        self.rocksclient = RocksClient(self.rocksdb_path, 'rw')

    def init_config_task(self):
        task_config = Vod.init_config_task(self)

        # config:task:rocksdb
        self.rocksdb_path = os.path.join(self.rocksdb_config['root'], self.rocksdb_config[self.task])

    def write_plus(self, _id, title, body_plus, doc_plus):
        self.rocksclient.put(_id, body_plus)
        self.logger.info('rocks put - %s', body_plus)

if __name__ == '__main__':
    faulthandler.enable()

    AlbumHeat(os.path.dirname(__file__)).start()
