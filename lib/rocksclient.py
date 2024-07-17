#!/usr/bin/env python3

import rocksdb
import os, sys

class RocksClient:

    def __init__(self, path, mode="r"):
        if mode == "rw":
            self.db = rocksdb.DB(path, rocksdb.Options(create_if_missing=True))
        else:
            self.db = rocksdb.DB(path, rocksdb.Options(create_if_missing=True), read_only=True)

    def str2bytes(self, data):
        if isinstance(data, str):
            return bytes(data, 'utf8')
        else:
            return bytes(str(data), 'utf8')

    def insert(self, id, doc):
        self.db.put(self.str2bytes(id), self.str2bytes(doc))

    def delete(self, id):
        self.db.delete(self.str2bytes(id))

    def update(self, id, doc):
        self.db.put(self.str2bytes(id), self.str2bytes(doc))

    def get(self, id):
        return self.db.get(self.str2bytes(id))

    def close(self):
        pass

if __name__ == "__main__":
    rocksclient = RocksClient(sys.argv[1])
    snapshot = rocksclient.db.snapshot()
    it = rocksclient.db.iteritems(snapshot = snapshot)

    it.seek_to_first()
    for key, doc in dict(it).items():
        print(key, doc)
