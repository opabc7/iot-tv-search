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

doc_sql_find = 'select id, body_plus from album limit %s, 100'
doc_sql_update = 'update album set body_plus = %s, update_time = %s where id = %s'

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
    mongo = MongoClient(mongo_conf)

    db_connection = pymysql.connect(host = db_host, port = db_port, user = db_user,
                                    password = db_password, database = db_database, charset = db_charset)

    #all, new = 0, 0
    #docs = mongo[mongo_db_name][mongo_table_name].find({}).batch_size(100)
    #for doc in docs:
    #    all += 1
#
    #    _id = doc['sid']
    #    title = doc['title']
    #    body = json.dumps(trans_mongo_doc_to_es(doc))
    #    print('mongo -', all, _id, title)

    #    with db_connection.cursor() as db_cursor:
    #        existed = db_cursor.execute(doc_sql_get, (_id, ))

    #    _time = int(time.time() * 1000)
    #    with db_connection.cursor() as db_cursor:
    #        if not existed:
    #            new += 1

    #            db_cursor.execute(doc_sql_insert, (_id, body, _time))
    #            print('db insert -', new, _id, title)

    #    db_connection.commit()

    offset = 0
    while True:
        with db_connection.cursor() as db_cursor:
            db_cursor.execute(doc_sql_find, (offset, ))
            results = db_cursor.fetchall()

        if results:
            for _id, body_plus in results:
                    if not body_plus:
                        print('mysql -', offset, _id)
                        doc = mongo[mongo_db_name][mongo_table_name_plus].find_one({'_id' : str(_id)})

                        if doc:
                            print('mongo -', _id, doc['title'])
                            body = json.dumps(trans_mongo_doc_to_es(doc))

                            _time = int(time.time() * 1000)
                            with db_connection.cursor() as db_cursor:
                                db_cursor.execute(doc_sql_update, (body, _time, _id))
                                print('db update -', _id)

                            db_connection.commit()
        else:
            break

        offset += 10
