#!/usr/bin/env python3

import sys
import rocksdb

class RocksClient:

    def __init__(self, path, mode="r"):
        if mode == "rw":
            self.db = rocksdb.DB(path, rocksdb.Options(create_if_missing=True))
        else:
            self.db = rocksdb.DB(path, rocksdb.Options(create_if_missing=True), read_only=True)

    def _str2bytes(self, data):
        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return bytes(data, 'utf8')
        else:
            return bytes(str(data), 'utf8')

    def put(self, _id, doc):
        self.db.put(self._str2bytes(_id), self._str2bytes(doc))

    def get(self, _id):
        return self.db.get(self._str2bytes(_id))

    def delete(self, _id):
        self.db.delete(self._str2bytes(_id))

if __name__ == "__main__":
    rocksclient = RocksClient(sys.argv[1])

    it = rocksclient.db.iteritems()
    it.seek_to_first()
    for item in it:
        print(item)
