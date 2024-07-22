#!/usr/bin/env python3

import os
import logging
import logging.config
import logging.handlers
import configparser
from rocketmq.client import PushConsumer, ConsumeStatus
import time
import json
import pymysql.cursors
import threading

class Vod:

    def __init__(self, work_dir, _id_field, title_field):
        self.db_connections = {}

        self.work_dir = work_dir
        self._id_field, self.title_field = _id_field, title_field

        self.init_logger()
        self.init_config()
        self.init_config_task()

    def init_logger(self):
        fpath = os.path.join(self.work_dir, 'conf', 'logging.ini')

        logging.config.fileConfig(fpath)
        self.logger = logging.getLogger()
        self.logger.info('logger created.')

    def init_config(self):
        fpath = os.path.join(self.work_dir, 'conf/config.ini')

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

        # config:log
        os.environ['vod_log_root'] = config['log']['root']

        # config:rocksdb
        self.rocksdb_config = {}
        for key in config['rocksdb']:
            self.rocksdb_config[key] = config['rocksdb'][key]

        # config:ext_data
        self.ext_data_config = {}
        for key in config['ext_data']:
            self.ext_data_config[key] = config['ext_data'][key]

    def init_config_task(self):
        fpath = os.path.join(self.work_dir, 'conf', self.task, 'config.ini')

        # config:task
        task_config = configparser.RawConfigParser()
        task_config.read(fpath)
        self.logger.info('%s config loaded.', self.task)

        # config:task:db
        self.doc_sql_query = task_config['db']['doc_sql_query']
        self.doc_sql_insert = task_config['db']['doc_sql_insert']
        self.doc_sql_update = task_config['db']['doc_sql_update']

        # config:task:mq
        self.mq_consumer_id = task_config['mq']['mq_consumer']
        self.mq_topic = task_config['mq']['mq_topic']

        return task_config

    def handle_mq(self, msg):
        try:
            body = msg.body.decode()

            doc = json.loads(body)
            _id, title = doc[self._id_field], None
            if self.title_field:
                title = doc[self.title_field]
            self.logger.info('received message - %s - %s', _id, title)

            doc_plus = self.process_doc(doc)
            body_plus = json.dumps(doc_plus)

            self.write_db(_id, title, body, body_plus)

            self.write_plus(_id, title, body_plus, doc_plus)

            self.logger.info('completed message - %s - %s', _id, title)
            return ConsumeStatus.CONSUME_SUCCESS
        except Exception as e:
            self.logger.error('failed message - %s', msg)
            self.logger.exception(e)
            return ConsumeStatus.RECONSUME_LATER

    def process_doc(self, doc):
        return doc

    def write_db(self, _id, title, body, body_plus):
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
            for_update = db_cursor.execute(self.doc_sql_query, (_id, ))

        _time = int(time.time() * 1000)
        with db_connection.cursor() as db_cursor:
            if for_update:
                db_cursor.execute(self.doc_sql_update, (body, _time, _id))
                self.logger.info('db update - %s - %s', _id, title)
            else:
                db_cursor.execute(self.doc_sql_insert, (_id, body, _time))
                self.logger.info('db insert - %s - %s', _id, title)

        db_connection.commit()

    def write_plus(self, _id, title, body_plus, doc_plus):
        pass

    def start(self):
        mq_consumer = PushConsumer(self.mq_consumer_id)
        mq_consumer.set_name_server_address(self.mq_addr)
        mq_consumer.subscribe(self.mq_topic, self.handle_mq)
        mq_consumer.start()
        self.logger.info('started to listen to mq: %s/%s/%s', self.mq_addr, self.mq_consumer_id, self.mq_topic)

        while True:
            self.logger.info('listening to mq: %s/%s/%s', self.mq_addr, self.mq_consumer_id, self.mq_topic)
            time.sleep(30)

        mq_consumer.shutdown()
