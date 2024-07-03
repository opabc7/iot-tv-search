#!/usr/bin/env python3

import os
import sys
import logging
import logging.config
import configparser
from rocketmq.client import PushConsumer, ConsumeStatus
import time
import json
import pymysql.cursors

db_connection = None

def handle_mq(msg):
    try:
        body = msg.body.decode()
        body_json = json.loads(body)
        sid = body_json['sid']
        title = body_json['title']
        logger.info('received message - %s - %s', sid, title)

        write_db(sid, title, body)

        write_es()

        logger.info('completed message - %s - %s', sid, title)
        return ConsumeStatus.CONSUME_SUCCESS
    except Exception as e:
        logger.error('failed message - %s - %s', sid, title)
        logger.exception(e)
        return ConsumeStatus.RECONSUME_LATER

def db_connect():
    if db_connection is None or not db_connection.open:
        db_connection = pymysql.connect(host = db_host, port = db_port, user = db_user,
                                            password = db_password, database = db_database ,
                                            charset = db_charset, cursorclass = pymysql.cursors.DictCursor)

        return db_connection

def write_db(sid, title, body):
    db_connection = db_connect()

    with db_connection.cursor() as db_cursor:
        db_cursor.execute(doc_query, (sid, ))
        for_update = db_cursor.fetchone()

    _time = int(time.time() * 1000)
    with db_connection.cursor() as db_cursor:
        if for_update:
            db_cursor.execute(doc_update, (body, _time, sid))
            logger.info('db update - %s - %s', sid, title)
        else:
            db_cursor.execute(doc_insert, (sid, body, _time))
            logger.info('db insert - %s - %s', sid, title)

    db_connection.commit()

def write_es():
    pass

if __name__ == '__main__':
    if len(sys.argv) > 1:
        task_name = sys.argv[1]
    else:
        raise Exception('no task assigned!')

    work_dir = os.path.dirname(__file__)

    # logger
    logging.config.fileConfig(os.path.join(work_dir, 'conf/logging.conf'))
    logger = logging.getLogger()
    logger.info('logger created.')

    # config
    config = configparser.ConfigParser()
    config.read(os.path.join(work_dir, 'conf/config.ini'))
    logger.info('config loaded.')

    # config:db
    db_host = config['db']['db_host']
    db_port = config['db']['db_port']
    db_user = config['db']['db_user']
    db_password = config['db']['db_password']
    db_database = config['db']['db_database']
    db_charset = config['db']['db_charset']

    # config:mq
    mq_addr = config['mq']['mq_addr']
    mq_consumer_id = config['mq']['mq_consumer']

    # config:task
    doc_query = config[task_name]['doc_query']
    doc_insert = config[task_name]['doc_insert']
    doc_update = config[task_name]['doc_update']
    mq_topic = config[task_name]['mq_topic']

    # mq
    mq_consumer = PushConsumer(mq_consumer_id)
    mq_consumer.set_name_server_address(mq_addr)
    mq_consumer.subscribe(mq_topic, handle_mq)
    mq_consumer.start()
    logger.info('started to listen to mq: %s %s %s', mq_addr, mq_consumer_id, mq_topic)

    while True:
        logger.info('listening to mq: %s %s %s', mq_addr, mq_consumer_id, mq_topic)
        time.sleep(30)

    mq_consumer.shutdown()
