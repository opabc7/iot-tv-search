#!/usr/bin/env python3

import time
import json
import pymysql.cursors
from lib.rocksclient import RocksClient

db_host = '10.79.0.66'
db_port = 33066
db_user = 'vod'
db_password = 'fRWKIpgUSgX5S_r-lQuZS0xJ0HStct_7r0VTOyB2-SbAQReIeOgCQYmRubXaDhaR'
db_database = 'cnvod'
db_charset = 'utf8mb4'

doc_sql_findone = 'select update_time from virtual_program where id = %s'
doc_sql_insert = 'insert into virtual_program(id, body, body_plus, create_time) values (%s, %s, %s, %s)'
doc_sql_update = 'update virtual_program set body = %s, body_plus = %s, update_time = %s where id = %s'

if __name__ == '__main__':
    migrate_time = int(time.time() * 1000)

    rocksclient = RocksClient('/data/apps/search/migrate/data/virtual_program/')
    db_connection = pymysql.connect(host = db_host, port = db_port, user = db_user,
                                    password = db_password, database = db_database, charset = db_charset)

    all, insert, update = 0, 0, 0
    it = rocksclient.db.iteritems()
    it.seek_to_first()
    for body in it:
        all += 1
        body_plus = body

        doc = json.loads(body)
        _id = doc['virtualSid']
        title = doc['title']
        print('rocks -', all, _id, title)

        with db_connection.cursor() as db_cursor:
            existed = db_cursor.execute(doc_sql_findone, (_id, ))

            (last_update_time,) = db_cursor.fetchone()
            if not last_update_time:
                last_update_time = 0

        _time = int(time.time() * 1000)
        with db_connection.cursor() as db_cursor:
            if existed:
                if last_update_time < migrate_time:
                    db_cursor.execute(doc_sql_update, (body, body_plus, _time, _id))

                    update += 1
                    print('db update -', update, _id, title)
            else:
                db_cursor.execute(doc_sql_insert, (_id, body, body_plus, _time))

                insert += 1
                print('db insert -', insert, _id, title)

        db_connection.commit()
