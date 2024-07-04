#!/usr/bin/env python3

import os
import sys
import logging
import logging.config
import logging.handlers
import configparser
from rocketmq.client import PushConsumer, ConsumeStatus
import time
import json
import pymysql.cursors
import threading

class vod:
    def __init__(self, work_dir):
        self.db_connections = {}

        self.init_logger(os.path.join(work_dir, 'conf/logging.conf'))
        self.init_config(os.path.join(work_dir, 'conf/config.ini'), os.path.join(work_dir, 'conf', task + '.ini'))

    def init_logger(self, fpath):
        logging.config.fileConfig(fpath)
        self.logger = logging.getLogger()
        self.logger.info('logger created.')

    def init_config(self, fpath, fpath_task):
        config = configparser.RawConfigParser()
        config.read(fpath)
        self.logger.info('config loaded.')

        # config:db
        self.db_host = config['db']['db_host']
        self.db_port = int(config['db']['db_port'])
        self.db_user = config['db']['db_user']
        self.db_password = config['db']['db_password']
        self.db_database = config['db']['db_database']
        self.db_charset = config['db']['db_charset']

        # config:mq
        self.mq_addr = config['mq']['mq_addr']
        self.mq_consumer_id = config['mq']['mq_consumer']

        # config:task
        task_config = configparser.RawConfigParser()
        task_config.read(fpath_task)
        self.logger.info('%s config loaded.', task)

        # config:task:db
        self.doc_sql_query = task_config['db']['doc_sql_query']
        self.doc_sql_insert = task_config['db']['doc_sql_insert']
        self.doc_sql_update = task_config['db']['doc_sql_update']

        # config:task:mq
        self.mq_topic = task_config['mq']['mq_topic']

    def handle_mq(self, msg):
        try:
            _id, title, body = self.pre(msg)

            self.write_db(id, title, body)

            self.write_es()

            self.logger.info('completed message - %s - %s', _id, title)
            return ConsumeStatus.CONSUME_SUCCESS
        except Exception as e:
            self.logger.error('failed message - %s - %s', _id, title)
            self.logger.exception(e)
            return ConsumeStatus.RECONSUME_LATER

    def pre(self, msg):
        body = msg.body.decode()

        self.logger.info('received message - %s', body)
        return None, None, body

    def write_db(self, id, title, body):
        _thread = threading.get_ident()

        if _thread in self.db_connections:
            db_connection = self.db_connections[_thread]

            if not db_connection.open:
                db_connection.connect()
        else:
            db_connection = pymysql.connect(host = self.db_host, port = self.db_port, user = self.db_user,
                                            password = self.db_password, database = self.db_database, charset = self.db_charset)

            self.db_connections[_thread] = db_connection

        with db_connection.cursor() as db_cursor:
            for_update = db_cursor.execute(self.doc_sql_query, (id, ))

        _time = int(time.time() * 1000)
        with db_connection.cursor() as db_cursor:
            if for_update:
                db_cursor.execute(self.doc_sql_update, (body, _time, id))
                self.logger.info('db update - %s - %s', id, title)
            else:
                db_cursor.execute(self.doc_sql_insert, (id, body, _time))
                self.logger.info('db insert - %s - %s', id, title)

        db_connection.commit()

    def write_es(self):
        pass

    def start(self, task):
        mq_consumer = PushConsumer(self.mq_consumer_id)
        mq_consumer.set_name_server_address(self.mq_addr)
        mq_consumer.subscribe(self.mq_topic, self.handle_mq)
        mq_consumer.start()
        self.logger.info('started to listen to mq: %s/%s/%s', self.mq_addr, self.mq_consumer_id, self.mq_topic)

        while True:
            self.logger.info('listening to mq: %s/%s/%s', self.mq_addr, self.mq_consumer_id, self.mq_topic)
            time.sleep(30)

        mq_consumer.shutdown()
