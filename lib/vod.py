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
        sid = doc_plus['sid']

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
            doc_plus['showTime'] = self.convert_showtime(sid, doc_plus['showTime'])

        self.gen_roles(sid, doc_plus)
        self.gen_feed(sid, doc_plus)

        self.gen_hot(sid, doc_plus)
        self.gen_virtual(sid, doc_plus)
        self.gen_season(sid, doc_plus)
        self.gen_mfield(sid, doc_plus)
        self.gen_series()
        self.gen_featureType()
        self.gen_title_digit_norm()
        self.gen_baidu_tags()
        self.gen_field_merge()
        self.gen_mtags()
        self.gen_minfo()
        self.gen_mtitle()
        self.remove_ghost_movie_tags()
        self.gen_douban_data()

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

    def gen_feed(self, sid, doc):
        try:
            featureType = doc['featureType']
            if featureType != 1:
                return

            copyrightCode = doc['copyrightCode']
            if copyrightCode != 'tencent':
                return

            contentType = doc['contentType']
            tags = doc['tags']
            sourceScore = doc['sourceScore']
            year = doc['year']
            area = doc['area']

            feed_tag = []
            # 豆瓣高分 电影、电视剧、综艺&豆瓣评分8.0以上从高排序
            if (contentType == "tv" or contentType == "movie" or contentType == "show") and sourceScore >= 8.0:
                feed_tag.append(u"豆瓣高分")
                # 热播大片 电影、电视剧&播放量&2020上映
                # TODO: show_count
            if (contentType == "tv" or contentType == "movie") and (year == 2020):
                feed_tag.append(u"热播大片")
            # 都市生活 电视剧、电影&都市
            if (contentType == "tv" or contentType == "movie") and (u"都市" in tags):
                feed_tag.append(u"都市生活")
            # 玄幻史诗 电视剧&玄幻、史诗
            if contentType == "tv" and (u"奇幻" in tags):
                feed_tag.append(u"玄幻史诗")
            # 青春校园 电视剧&青春
            if contentType == "tv" and (u"青春" in tags):
                feed_tag.append(u"青春校园")
            # 军旅抗战 电视剧&军旅、抗战、抗日、谍战
            if contentType == "tv" and (u"军旅" in tags or u"抗战" in tags or u"抗日" in tags or u"谍战" in tags):
                feed_tag.append(u"军旅抗战")
            # 爆笑喜剧 电视剧、电影、综艺&搞笑、喜剧
            if (contentType == "tv" or contentType == "movie" or contentType == u"show") and (u"搞笑" in tags or u"喜剧" in tags):
                feed_tag.append(u"爆笑喜剧")
            # 武侠江湖 电视剧、电影&武侠、江湖
            if (contentType == "tv" or contentType == "movie") and (u"武侠" in tags or u"江湖" in tags):
                feed_tag.append(u"武侠江湖")
            # 劲爆美剧 电视剧、电影&美国&美国
            if (contentType == "tv") and (u"美国" == area):
                feed_tag.append(u"劲爆美剧")
            # 古装历史 电视剧、电影&古装、历史
            if (contentType == "tv" or contentType == "movie") and (u"古装" in tags or u"历史" in tags):
                feed_tag.append(u"古装历史")
            # 动画电影 电影&动画
            if (contentType == "movie") and (u"动画" in tags):
                feed_tag.append(u"动画电影")
            # 悬疑惊悚 电影&悬疑、惊悚、恐怖
            if (contentType == "movie") and (u"悬疑" in tags or u"惊悚" in tags or u"恐怖" in tags):
                feed_tag.append(u"悬疑惊悚")
            # 真人秀场 综艺&脱口秀、养成、体验
            if (contentType == "show") and (u"脱口秀" in tags or u"养成" in tags or u"体验" in tags):
                feed_tag.append(u"真人秀场")
            # 国漫精品 动漫&中国、内地&热血、经典
            if (contentType == "comic") and (area == u"内地" or area == u"中国") and (u"热血" in tags or u"经典" in tags):
                feed_tag.append(u"国漫精品")
            # 经典日漫 动漫&日本
            if (contentType == "comic" and area == u"日本"):
                feed_tag.append(u"经典日漫")
            # 儿童益智 少儿&益智、绘画、早教
            if (contentType == "kids") and (u"益智" in tags or u"绘画" in tags or u"早教" in tags):
                feed_tag.append(u"儿童益智")
            # 手工绘画 少儿&手工、绘画、玩具
            if (contentType == "kids") and (u"手工" in tags or u"绘画" in tags or u"玩具" in tags):
                feed_tag.append(u"手工绘画")
            # 儿歌天地 少儿&儿歌
            if (contentType == "kids") and (u"儿歌" in tags or u"儿童音乐" in tags):
                feed_tag.append(u"儿歌天地")
            # 真人特摄 少儿&真人
            if (contentType == "kids") and (u"真人" in tags):
                feed_tag.append(u"真人特摄")
            # 舌尖美食 纪录片&美食
            if (contentType == "doc") and (u"美食" in tags):
                feed_tag.append(u"舌尖美食")
            # 自然万象 纪录片&自然、动物
            if (contentType == "doc") and (u"自然" in tags or u"动物" in tags):
                feed_tag.append(u"自然万象")
            # 探索科技 纪录片&科技、探索
            if (contentType == "doc") and (u"科技" in tags or u"探索" in tags):
                feed_tag.append(u"探索科技")
            # 历史人文 纪录片&历史、人文
            if (contentType == "doc") and (u"历史" in tags or u"人文" in tags):
                feed_tag.append(u"历史人文")
            # 旅游天地 纪录片&旅游
            if (contentType == "doc") and (u"旅游" in tags):
                feed_tag.append(u"旅游天地")
            # 儿童精品 少儿&合家欢
            if (contentType == "kids") and (u"合家欢" in tags):
                feed_tag.append(u"儿童精品")
            # 偶像爱情 电视剧&爱情、都市、偶像
            if (contentType == "tv") and (u"爱情" in tags or u"都市" in tags or u"偶像" in tags):
                feed_tag.append(u"偶像爱情")

            doc["feed_tag"] = feed_tag
        except Exception as e:
            self.logger.error('gen feed failed - %s', sid)
            self.logger.exception(e)

    def gen_hot(self, sid, doc):
        pass

    def gen_virtual(self, sid, doc):
        pass

    def gen_season(self, sid, doc):
        pass

    def gen_mfield(self, sid, doc):
        pass

    def gen_series(self):
        pass

    def gen_featureType(self):
        pass

    def gen_title_digit_norm(self):
        pass

    def gen_baidu_tags(self):
        pass

    def gen_field_merge(self):
        pass

    def gen_mtags(self):
        pass

    def gen_minfo(self):
        pass

    def gen_mtitle(self):
        pass

    def remove_ghost_movie_tags(self):
        pass

    def gen_douban_data(self):
        pass
