#!/usr/bin/env python3

import time
import json
import pymysql.cursors
from pymongo import MongoClient

mongo_conf =  'mongodb://tv_album_rw:UOFq05UWzAoBQJaTcS2Hrugrdv_fmaRhzOVY2K01I6tP4xGzNq1TAzlZpR1dm7r5@tv-vod01.mongodb.oppo.local:20000,tv-vod02.mongodb.oppo.local:20000,tv-vod03.mongodb.oppo.local:20000/tv_album'
mongo_db_name = 'tv_album'
mongo_table_name = 'vod_album_info'
mongo_table_name_plus = 'vod_album_info_alg'

db_host = '10.79.0.66'
db_port = 33066
db_user = 'vod'
db_password = 'fRWKIpgUSgX5S_r-lQuZS0xJ0HStct_7r0VTOyB2-SbAQReIeOgCQYmRubXaDhaR'
db_database = 'cnvod'
db_charset = 'utf8mb4'

doc_sql_get = 'select id from album where id = %s'
doc_sql_insert = 'insert into album(id, body, create_time) values (%s, %s, %s)'

doc_sql_find = 'select id, title, body_plus from album where limit %s,10'
doc_sql_update = 'update album set body_plus = %s, update_time = %s where id = %s'

if __name__ == '__main__':
    mongo = MongoClient(mongo_conf)

    db_connection = pymysql.connect(host = db_host, port = db_port, user = db_user,
                                    password = db_password, database = db_database, charset = db_charset)

    docs = mongo.find(mongo_db_name, mongo_table_name)
    for doc in docs:
        _id = doc['sid']
        title = doc['title']
        body = json.dumps(doc)

        print('mongo', _id, title)

    offset = 0
    while True:
        with db_connection.cursor() as db_cursor:
            sum = db_cursor.execute(doc_sql_find, (offset, ))
            if sum:
                for _id, title, body_plus in db_cursor.fetchall():
                    print('mysql', _id, title)
            else:
                break

            offset += 10
