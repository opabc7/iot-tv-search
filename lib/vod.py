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

class Vod:
    def __init__(self, work_dir, task, _id_field, title_field):
        self.db_connections = {}

        self._id_field, self.title_field = _id_field, title_field
        self.init_logger(os.path.join(work_dir, 'conf', task, 'logging.ini'))
        self.init_config(os.path.join(work_dir, 'conf/config.ini'), task, os.path.join(work_dir, 'conf', task, 'config.ini'))

    def init_logger(self, fpath):
        logging.config.fileConfig(fpath)
        self.logger = logging.getLogger()
        self.logger.info('logger created.')

    def init_config(self, fpath, task, fpath_task):
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

        # config:task
        task_config = configparser.RawConfigParser()
        task_config.read(fpath_task)
        self.logger.info('%s config loaded.', task)

        # config:task:db
        self.doc_sql_query = task_config['db']['doc_sql_query']
        self.doc_sql_insert = task_config['db']['doc_sql_insert']
        self.doc_sql_update = task_config['db']['doc_sql_update']

        # config:task:mq
        self.mq_consumer_id = task_config['mq']['mq_consumer']
        self.mq_topic = task_config['mq']['mq_topic']

    def handle_mq(self, msg):
        try:
            body = msg.body.decode()

            doc = json.loads(body)
            _id, title = doc[self._id_field], doc[self.title_field]
            self.logger.info('received message - %s - %s', _id, title)

            doc_plus = self.plus(doc)

            self.write_db(_id, title, body, json.dumps(doc_plus))

            self.write_es(doc_plus)

            self.logger.info('completed message - %s - %s', _id, title)
            return ConsumeStatus.CONSUME_SUCCESS
        except Exception as e:
            self.logger.error('failed message - %s', msg)
            self.logger.exception(e)
            return ConsumeStatus.RECONSUME_LATER

    def plus(self, doc):
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

    def write_es(self, doc):
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

class Person(Vod):
    def __init__(self, work_dir):
       Vod.__init__(self, work_dir, 'person', 'sid', 'title')

class VirtualProgram(Vod):
    def __init__(self, work_dir):
       Vod.__init__(self, work_dir, 'virtual_program', 'virtualSid', 'title')

class Album(Vod):
    def __init__(self, work_dir):
       Vod.__init__(self, work_dir, 'album', 'sid', 'title')

    def plus(self, doc):
        doc_plus = doc.copy()

        if 'tags' in doc_plus and doc_plus['tags']:
            doc_plus['tags'] = doc_plus['tags'].split("|")
        if "cid" not in doc_plus and "sourceAlbumId" in doc_plus:
            doc_plus["cid"] = doc_plus["sourceAlbumId"]

        doc_plus['episodes.brief'] = []
        doc_plus['episodes.copyrightCode'] = []
        doc_plus['episodes.createTime'] = []
        doc_plus['episodes.eid'] = []
        doc_plus['episodes.episode'] = []
        doc_plus['episodes.episodeIndex'] = []
        doc_plus['episodes.featureType'] = []
        doc_plus['episodes.horizontalIcon'] = []
        doc_plus['episodes.linkType'] = []
        doc_plus['episodes.linkValue'] = []
        doc_plus['episodes.markCode'] = []
        doc_plus['episodes.productCode'] = []
        doc_plus['episodes.productName'] = []
        doc_plus['episodes.status'] = []
        doc_plus['episodes.subscriptCode'] = []
        doc_plus['episodes.title'] = []
        doc_plus['episodes.updateTime'] = []
        doc_plus['episodes.verticalIcon'] = []
        doc_plus['episodes.vipType'] = []
        if 'episodes' in doc_plus and doc_plus['episodes']:
            for episode in doc_plus['episodes']:
                if 'brief' in episode:
                    doc_plus['episodes.brief'].append(episode['brief'])
                if 'copyrightCode' in episode:
                    doc_plus['episodes.copyrightCode'].append(episode['copyrightCode'])
                if 'createTime' in episode:
                    doc_plus['episodes.createTime'].append(episode['createTime'])
                if 'eid' in episode:
                    doc_plus['episodes.eid'].append(episode['eid'])
                if 'episode' in episode:
                    doc_plus['episodes.episode'].append(episode['episode'])
                if 'episodeIndex' in episode:
                    doc_plus['episodes.episodeIndex'].append(episode['episodeIndex'])
                if 'featureType' in episode:
                    doc_plus['episodes.featureType'].append(episode['featureType'])
                if 'horizontalIcon' in episode:
                    doc_plus['episodes.horizontalIcon'].append(episode['horizontalIcon'])
                if 'linkType' in episode:
                    doc_plus['episodes.linkType'].append(episode['linkType'])
                if 'linkValue' in episode:
                    doc_plus['episodes.linkValue'].append(episode['linkValue'])
                if 'markCode' in episode:
                    doc_plus['episodes.markCode'].append(episode['markCode'])
                if 'productCode' in episode:
                    doc_plus['episodes.productCode'].append(episode['productCode'])
                if 'productName' in episode:
                    doc_plus['episodes.productName'].append(episode['productName'])
                if 'status' in episode:
                    doc_plus['episodes.status'].append(episode['status'])
                if 'subscriptCode' in episode:
                    doc_plus['episodes.subscriptCode'].append(episode['subscriptCode'])
                if 'title' in episode:
                    doc_plus['episodes.title'].append(episode['title'])
                if 'updateTime' in episode:
                    doc_plus['episodes.updateTime'].append(episode['updateTime'])
                if 'verticalIcon' in episode:
                    doc_plus['episodes.verticalIcon'].append(episode['verticalIcon'])
                if 'vipType' in episode:
                    doc_plus['episodes.vipType'].append(episode['vipType'])

        doc_plus['persons.headImg'] = []
        doc_plus['persons.personName'] = []
        doc_plus['persons.personSid'] = []
        doc_plus['persons.roleImg'] = []
        doc_plus['persons.roleName'] = []
        doc_plus['persons.roleType'] = []
        doc_plus['persons.source'] = []
        doc_plus['persons.squareImg'] = []
        if 'persons' in doc_plus and doc_plus['persons']:
            for person in doc_plus['persons']:
                if 'headImg' in person:
                    doc_plus['persons.headImg'].append(person['headImg'])
                if 'personName' in person:
                    doc_plus['persons.personName'].append(person['personName'])
                if 'personSid' in person:
                    doc_plus['persons.personSid'].append(person['personSid'])
                if 'roleImg' in person:
                    doc_plus['persons.roleImg'].append(person['roleImg'])
                if 'roleName' in person:
                    doc_plus['persons.roleName'].append(person['roleName'])
                if 'roleType' in person:
                    doc_plus['persons.roleType'].append(person['roleType'])
                if 'source' in person:
                    doc_plus['persons.source'].append(person['source'])
                if 'squareImg' in person:
                    doc_plus['persons.squareImg'].append(person['squareImg'])

        if 'showTime' in doc_plus:
            doc_plus['showTime'] = self.convert_showtime(doc_plus['sid'], doc_plus['showTime'])

        self.gen_roles(doc_plus['sid'], doc_plus)

        return doc_plus

    def convert_showtime(self, sid, showtime):
        try:
            if showtime == "" or showtime == "0000-00-00 00:00:00" or len(showtime.split(' ')) == 3:
                self.logger.error('unknown time format - %s - %s', sid, showtime)
                return "2020-01-01 00:00:00"

            if len(showtime.split(' ')) == 1:
                try:
                    timeArray = time.strptime(showtime, "%Y-%m-%d")
                    showtime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                except Exception as e:
                    self.logger.error('unknown time format - %s - %s', sid, showtime)
                    self.logger.exception(e)

                    try:
                        timeArray = time.strptime(showtime, "%Y%m")
                        showtime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                    except Exception as e:
                        self.logger.error('unknown time format - %s - %s', sid, showtime)
                        self.logger.exception(e)
                        return "2020-01-01 00:00:00"

                return showtime

            if len(showtime.split(' ')) == 2:
                timeArray = time.strptime(showtime, "%Y-%m-%d %H:%M:%S")
                showtime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                return showtime
        except Exception as e:
            self.logger.error('unknown time format - %s - %s', sid, showtime)
            self.logger.exception(e)
            return "2020-01-01 00:00:00"

    def gen_roles(self, sid, doc):
        roles = {
            1 : "导演",
            2 : "演员",
            3 : "编剧",
            4 : "制片人",
        }

        doc['stars'] = []
        doc['directors'] = []
        if 'persons.personName' not in doc or 'persons.roleType' not in doc:
            self.logger.error("persons has no personName or roleType - %s", sid)
            return

        personName = doc['persons.personName']
        roleType = doc['persons.roleType']
        if len(personName) != len(roleType):
            self.logger.error("persons and roleType size not equal - %s", sid)
            return

        for index, value in enumerate(roleType):
            if value == 1:
                doc['directors'].append(personName[index])
            if value == 2:
                doc['stars'].append(personName[index])
