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

doc_sql_findone = 'select update_time from album where id = %s'
doc_sql_insert_body = 'insert into album(id, body, create_time) values (%s, %s, %s)'
doc_sql_update_body_plus = 'update album set body_plus = %s, update_time = %s where id = %s'

def trans_mongo_doc_to_es(doc):
    for k, v in doc.copy().items():
        if '__' in k:
            new_k = k.replace('__', '.')

            doc[new_k] = v

            del doc[k]

        if k == '_id':
            del doc[k]

    return doc

if __name__ == '__main__':
    migrate_time = int(time.time() * 1000)

    mongo = MongoClient(mongo_conf)
    db_connection = pymysql.connect(host = db_host, port = db_port, user = db_user,
                                    password = db_password, database = db_database, charset = db_charset)

    all, insert, update = 0, 0, 0
    docs = mongo[mongo_db_name][mongo_table_name].find({}).batch_size(100)
    for doc in docs:
        all += 1

        _id = doc['sid']
        title = doc['title']
        body = json.dumps(trans_mongo_doc_to_es(doc))
        print('mongo -', all, _id, title)

        with db_connection.cursor() as db_cursor:
            existed = db_cursor.execute(doc_sql_findone, (_id, ))

            last_update_time = db_cursor.fetchone()
            if not last_update_time:
                last_update_time = 0

        _time = int(time.time() * 1000)
        with db_connection.cursor() as db_cursor:
            if existed:
                if last_update_time < migrate_time:
                    doc_plus = mongo[mongo_db_name][mongo_table_name_plus].find_one({'_id' : _id})
                    body_plus = json.dumps(trans_mongo_doc_to_es(doc_plus))

                    db_cursor.execute(doc_sql_update_body_plus, (body_plus, _time, _id))

                    update += 1
                    print('db update -', update, _id, title)
            else:
                db_cursor.execute(doc_sql_insert_body, (_id, body, _time))

                insert += 1
                print('db insert -', insert, _id, title)

        db_connection.commit()
