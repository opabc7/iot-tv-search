#!/usr/bin/env python3

import faulthandler
import os
import time
import demjson3
import re
from lib.rocksclient import RocksClient
from lib.utils import jsonutils
from vod.handler import VodHandler
from vod.vod_title_extractor import TitleExtractor
from vod.vod_added_dataloader import VodAddedDataloader
from vod import vod_heat_gererator
from vod import vod_datadict
from elasticsearch import Elasticsearch
from mappings import vod_album_mapping

featureType_pattern1 = r"花絮|精彩集锦|片段|彩蛋|预告|排行|首映礼|精彩看点|纪录片|特别纪录|混剪|名场面|周诌大电影"
featureType_pattern2 = r"幕后"

class AlbumHandler(VodHandler):

    def __init__(self, work_dir):
        self.task = os.environ['vod_task'] = 'album'

        VodHandler.__init__(self, work_dir, 'sid', 'title')

        self.rocksclient_heat = RocksClient(self.rocksdb_path_heat)
        self.rocksclient_virtual_program = RocksClient(self.rocksdb_path_virtual_program)

        added_dataloader = VodAddedDataloader(self.logger, self.added_data_config)
        self.series_map = added_dataloader.get_sereis_map()
        self.featuretype_map = added_dataloader.get_featuretype_map()
        self.title_digit_norm_map = added_dataloader.get_title_digit_norm_map()
        self.baidu_tags_map = added_dataloader.get_baidu_tags_map()
        self.ghost_tags_map = added_dataloader.get_ghost_tags_map()
        self.douban_map = added_dataloader.get_douban_map()
        self.field_merge_map = added_dataloader.get_field_merge_map()

        self.es = Elasticsearch(self.es_hosts)
        if self.es.indices.exists(index = self.index_name):
            self.logger.info("search index %s has already existed.", self.index_name)
        else:
            es_res = self.es.indices.create(index = self.index_name, body = vod_album_mapping.mappings)
            self.logger.info("search index created, result: %s", es_res)

    def init_config_task(self):
        task_config = VodHandler.init_config_task(self)

        # config:task:es
        self.index_name = task_config['es']['index_name']

        # config:task:rocksdb
        self.rocksdb_path_heat = os.path.join(self.rocksdb_config['root'], self.rocksdb_config[task_config['rocksdb']['heat']])
        self.rocksdb_path_virtual_program = os.path.join(self.rocksdb_config['root'], self.rocksdb_config[task_config['rocksdb']['virtual_program']])

    def process_doc(self, doc):
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
        self.gen_series(sid, doc_plus)
        self.gen_featureType(sid, doc_plus)
        self.gen_title_digit_norm(sid, doc_plus)
        self.gen_baidu_tags(sid, doc_plus)
        self.gen_field_merge(sid, doc_plus)
        self.gen_mtags(sid, doc_plus)
        self.gen_minfo(sid, doc_plus)
        self.gen_mtitle(sid, doc_plus)
        self.remove_ghost_movie_tags(sid, doc_plus)
        self.gen_douban_data(sid, doc_plus)

        return doc_plus

    def write_plus(self, _id, title, body_plus, doc_plus):
        self.rocksclient.put(_id, body_plus)
        self.logger.info('rocks put - %s', body_plus)

        try:
            es_res = self.es.index(index = self.index_name, id = _id, body = doc_plus)
            self.logger.info("search update succeded: %s", es_res)
        except Exception as e:
            self.logger.error("search update failed: %s", body_plus)
            self.logger.exception(e)

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
                feed_tag.append("豆瓣高分")
                # 热播大片 电影、电视剧&播放量&2020上映
                # TODO: show_count
            if (contentType == "tv" or contentType == "movie") and (year == 2020):
                feed_tag.append("热播大片")
            # 都市生活 电视剧、电影&都市
            if (contentType == "tv" or contentType == "movie") and ("都市" in tags):
                feed_tag.append("都市生活")
            # 玄幻史诗 电视剧&玄幻、史诗
            if contentType == "tv" and ("奇幻" in tags):
                feed_tag.append("玄幻史诗")
            # 青春校园 电视剧&青春
            if contentType == "tv" and ("青春" in tags):
                feed_tag.append("青春校园")
            # 军旅抗战 电视剧&军旅、抗战、抗日、谍战
            if contentType == "tv" and ("军旅" in tags or "抗战" in tags or "抗日" in tags or "谍战" in tags):
                feed_tag.append("军旅抗战")
            # 爆笑喜剧 电视剧、电影、综艺&搞笑、喜剧
            if (contentType == "tv" or contentType == "movie" or contentType == "show") and ("搞笑" in tags or "喜剧" in tags):
                feed_tag.append("爆笑喜剧")
            # 武侠江湖 电视剧、电影&武侠、江湖
            if (contentType == "tv" or contentType == "movie") and ("武侠" in tags or "江湖" in tags):
                feed_tag.append("武侠江湖")
            # 劲爆美剧 电视剧、电影&美国&美国
            if (contentType == "tv") and ("美国" == area):
                feed_tag.append("劲爆美剧")
            # 古装历史 电视剧、电影&古装、历史
            if (contentType == "tv" or contentType == "movie") and ("古装" in tags or "历史" in tags):
                feed_tag.append("古装历史")
            # 动画电影 电影&动画
            if (contentType == "movie") and ("动画" in tags):
                feed_tag.append("动画电影")
            # 悬疑惊悚 电影&悬疑、惊悚、恐怖
            if (contentType == "movie") and ("悬疑" in tags or "惊悚" in tags or "恐怖" in tags):
                feed_tag.append("悬疑惊悚")
            # 真人秀场 综艺&脱口秀、养成、体验
            if (contentType == "show") and ("脱口秀" in tags or "养成" in tags or "体验" in tags):
                feed_tag.append("真人秀场")
            # 国漫精品 动漫&中国、内地&热血、经典
            if (contentType == "comic") and (area == "内地" or area == "中国") and ("热血" in tags or "经典" in tags):
                feed_tag.append("国漫精品")
            # 经典日漫 动漫&日本
            if (contentType == "comic" and area == "日本"):
                feed_tag.append("经典日漫")
            # 儿童益智 少儿&益智、绘画、早教
            if (contentType == "kids") and ("益智" in tags or "绘画" in tags or "早教" in tags):
                feed_tag.append("儿童益智")
            # 手工绘画 少儿&手工、绘画、玩具
            if (contentType == "kids") and ("手工" in tags or "绘画" in tags or "玩具" in tags):
                feed_tag.append("手工绘画")
            # 儿歌天地 少儿&儿歌
            if (contentType == "kids") and ("儿歌" in tags or "儿童音乐" in tags):
                feed_tag.append("儿歌天地")
            # 真人特摄 少儿&真人
            if (contentType == "kids") and ("真人" in tags):
                feed_tag.append("真人特摄")
            # 舌尖美食 纪录片&美食
            if (contentType == "doc") and ("美食" in tags):
                feed_tag.append("舌尖美食")
            # 自然万象 纪录片&自然、动物
            if (contentType == "doc") and ("自然" in tags or "动物" in tags):
                feed_tag.append("自然万象")
            # 探索科技 纪录片&科技、探索
            if (contentType == "doc") and ("科技" in tags or "探索" in tags):
                feed_tag.append("探索科技")
            # 历史人文 纪录片&历史、人文
            if (contentType == "doc") and ("历史" in tags or "人文" in tags):
                feed_tag.append("历史人文")
            # 旅游天地 纪录片&旅游
            if (contentType == "doc") and ("旅游" in tags):
                feed_tag.append("旅游天地")
            # 儿童精品 少儿&合家欢
            if (contentType == "kids") and ("合家欢" in tags):
                feed_tag.append("儿童精品")
            # 偶像爱情 电视剧&爱情、都市、偶像
            if (contentType == "tv") and ("爱情" in tags or "都市" in tags or "偶像" in tags):
                feed_tag.append("偶像爱情")

            doc["feed_tag"] = feed_tag
            self.logger.info('gen feed - %s - %s', sid, feed_tag)
        except Exception as e:
            self.logger.error('gen feed failed - %s', sid)
            self.logger.exception(e)

    def gen_hot(self, sid, doc):
        hot_info_str = sid

        try:
            hot_info_str = self.rocksclient_heat.get(sid)

            if hot_info_str is None:
                score = 0.0
                playNum = 0

                doc['hot'], doc['playNum'] = score, playNum
                self.logger.info('gen hot - %s - %s - %s', sid, score, playNum)
            else:
                hot_info = demjson3.decode(hot_info_str)
                playNum = hot_info['playNum']

                if 'source' in hot_info:
                    score = vod_heat_gererator.get(hot_info['source'], hot_info)
                else:
                    score = 0.0

                doc['hot'] = score
                doc['playNum'] = playNum
                self.logger.info('gen hot - %s - %s - %s', sid, score, playNum)
        except Exception as e:
            self.logger.error('gen hot failed - %s', hot_info_str)
            self.logger.exception(e)

    def gen_virtual(self, sid, doc):
        virtual_info_str = sid

        try:
            if doc['status'] != 1 or doc['featureType'] != 1:
                return

            virtual_info_str = self.rocksclient_virtual_program.get(sid)
            if virtual_info_str is None:
                return

            virtual_info = demjson3.decode(virtual_info_str)
            virtual_status = virtual_info['status']
            if virtual_status != 1:
                return

            doc['virtualSid'] = virtual_info['virtualSid']

            pri_this = 0
            src_this = doc['copyrightCode']
            if src_this in vod_datadict.source_priority:
                pri_this = vod_datadict.source_priority[src_this]

            hasBigger = False
            programs = virtual_info['virtualProgramRelList']
            for program in programs:
                src_other = program['source']
                if src_other == src_this:
                    continue

                program_status = program['status']
                if program_status != 1:
                    continue

                pri_other = 0
                if src_other in vod_datadict.source_priority:
                    pri_other = vod_datadict.source_priority[src_other]

                if pri_other > pri_this:
                    hasBigger = True
                    break

            if hasBigger:
                doc['status'] = -1
                doc['virtual_status'] = 0

                self.logger.info('gen virtual - %s - %s', sid, 'inactive')
            else:
                doc['virtual_status'] = 1
                self.logger.info('gen virtual - %s - %s', sid, 'active')
        except Exception as e:
            self.logger.error('gen virtual failed - %s', virtual_info_str)
            self.logger.exception(e)

    def gen_season(self, sid, doc):
        try:
            title_extractor = TitleExtractor(doc['title'], doc['contentType'])
            title_extractor.parse_name_season()

            name = title_extractor.get_name()
            season = title_extractor.get_season()
            if season >= 2 ** 31 - 1:
                season = -1

            doc['name'] = name
            doc['season'] = season
            self.logger.info('gen season - %s - %s - %s', sid, name, season)
        except Exception as e:
            self.logger.error('gen season failed - %s - %s - %s', sid, doc['title'], doc['contentType'])
            self.logger.exception(e)

    def gen_mfield(self, sid, doc):
        try:
            m_field = ""

            title = jsonutils.get_value_with_default(doc, 'title', str)
            if title:
                m_field += title + " | "

            contentType = jsonutils.get_value_with_default(doc, 'contentType', str)
            if contentType and contentType in vod_datadict.contentType_dict:
                m_field += vod_datadict.contentType_dict[contentType] + " | "

            tags = doc['tags']
            if tags:
                for t in tags:
                    m_field += t + " | "

            feed_tag = jsonutils.get_value_with_default(doc, 'feed_tag', list)
            if feed_tag:
                for tag in feed_tag:
                    m_field += tag + " | "

            area = jsonutils.get_value_with_default(doc, 'area', str)
            if area:
                m_field += area + " | "

            brief = jsonutils.get_value_with_default(doc, 'brief', str)
            if brief:
                m_field += brief + " | "

            information = jsonutils.get_value_with_default(doc, 'information', str)
            if information:
                m_field += information + " | "

            language = jsonutils.get_value_with_default(doc, 'language', str)
            if language:
                m_field += language + " | "

            name = jsonutils.get_value_with_default(doc, 'name', str)
            if name:
                m_field += name + " | "

            if 'persons' in doc and doc['persons']:
                persons = doc['persons']
                for person in persons:
                    person_name = person['personName']
                    if person_name:
                        m_field += person_name + " | "

                    role_name = person['roleName']
                    if role_name:
                        m_field += role_name + " | "

            doc['m_field'] = m_field
            self.logger.info('gen mfield - %s - %s', sid, m_field)
        except Exception as e:
            self.logger.error('gen mfield failed - %s', sid)
            self.logger.exception(e)

    def gen_series(self, sid, doc):
        try:
            if sid in self.series_map:
                series_name = self.series_map[sid]['series_name']
                series_rank = self.series_map[sid]['series_rank']
            else:
                series_name = ""
                series_rank = -1

            doc['series_name'] = series_name
            doc['series_rank'] = int(series_rank)
            self.logger.info('gen series - %s - %s - %s', sid, series_name, series_rank)
        except Exception as e:
            self.logger.error('gen series failed - %s', sid)
            self.logger.exception(e)

    def gen_featureType(self, sid, doc):
        try:
            newFeatureType = None

            status = doc["status"]
            if status == 1:
                if sid in self.featuretype_map:
                    newFeatureType = self.featuretype_map[sid]
                else:
                    featureType = doc["featureType"]

                    if featureType == 1:
                        contentType = doc["contentType"]

                        if contentType in ("movie", "tv", "comic", "show", "child", "kids", "doc", "edu"):
                            source = jsonutils.get_value_with_default(doc, "source", str).strip()
                            sourceScore = jsonutils.get_value_with_default(doc, "sourceScore", int)
                            episode = jsonutils.get_value_with_default(doc, "episode", int)
                            year = jsonutils.get_value_with_default(doc, "year", int)

                            if (contentType == "doc" or contentType == "show") and source == "iqiyi" and (sourceScore is None or sourceScore < 0.01):
                                newFeatureType = 5
                            elif episode == 1 and (year is None or year == 0) and (sourceScore is None or sourceScore < 0.01):
                                newFeatureType = 5
                        elif contentType in ("movie", "tv", "comic", "show", "child", "kids"):
                            title = doc["title"]
                            source = jsonutils.get_value_with_default(doc, "source", str).strip()
                            episode = jsonutils.get_value_with_default(doc, "episode", int)
                            duration = jsonutils.get_value_with_default(doc, "duration", int)
                            verticalIcon = jsonutils.get_value_with_default(doc, "verticalIcon", str).strip()

                            if re.search(featureType_pattern1, title):
                                if title not in ("预告犯", "业余纪录片"):
                                    newFeatureType = 5
                            elif re.search(featureType_pattern2, title) and "《" in title:
                                newFeatureType = 5
                            elif not verticalIcon:
                                newFeatureType = 5
                            elif contentType == "movie" and source == "youku" and episode == 0:
                                newFeatureType = 5
                            elif contentType == "movie" and source == "iqiyi" and duration < 600:
                                newFeatureType = 5

            if newFeatureType:
                doc["featureType"] = newFeatureType
                self.logger.info('gen featureType - %s - %s', sid, newFeatureType)
        except Exception as e:
            self.logger.error('gen featureType failed - %s', sid)
            self.logger.exception(e)

    def gen_title_digit_norm(self, sid, doc):
        try:
            if sid in self.title_digit_norm_map:
                normalize_title = self.title_digit_norm_map[sid].strip().split("|")
                normalize_title = " | ".join(normalize_title)
            else:
                normalize_title = ""

            doc["normalize_title"] = normalize_title
            self.logger.info('gen title_digit_norm - %s - %s', sid, normalize_title)
        except Exception as e:
            self.logger.error('gen title_digit_norm failed - %s', sid)
            self.logger.exception(e)

    def gen_baidu_tags(self, sid, doc):
        try:
            if sid in self.baidu_tags_map:
                baidu_tags = self.baidu_tags_map[sid]
                baidu_tags = " | ".join(baidu_tags)
            else:
                baidu_tags = ""

            doc["baidu_tags"] = baidu_tags
            self.logger.info('gen baidu_tags - %s - %s', sid, baidu_tags)
        except Exception as e:
            self.logger.error('gen baidu_tags failed - %s', sid)
            self.logger.exception(e)

    def gen_field_merge(self, sid, doc):
        ''' 
        增加字段 merge_tags、merge_area、merge_personsName、merge_language、merge_contentType_mapping、merge_year
        '''
        try:
            if sid in self.field_merge_map:
                field_merge_info = self.field_merge_map[sid]

                merge_tags = jsonutils.get_value_with_default(field_merge_info, "merge_tags", list)
                if len(merge_tags):
                    doc['merge_tags'] = " | ".join(merge_tags)
                    self.logger.info('gen merge_tags - %s - %s', sid, doc['merge_tags'])

                merge_area = jsonutils.get_value_with_default(field_merge_info, "merge_area", list)
                if len(merge_area):
                    doc['merge_area'] = merge_area
                    self.logger.info('gen merge_area - %s - %s', sid, doc['merge_area'])

                merge_personsName = jsonutils.get_value_with_default(field_merge_info, "merge_personsName", list)
                if len(merge_personsName):
                    doc['merge_personsName'] = " | ".join(merge_personsName)
                    self.logger.info('gen merge_personsName - %s - %s', sid, doc['merge_personsName'])

                merge_language = jsonutils.get_value_with_default(field_merge_info, "merge_language", list)
                if len(merge_language):
                    doc['merge_language'] = " | ".join(merge_language)
                    self.logger.info('gen merge_language - %s - %s', sid, doc['merge_language'])

                merge_contentType_mapping = jsonutils.get_value_with_default(field_merge_info, "merge_contentType_mapping", list)
                if len(merge_contentType_mapping):
                    doc['merge_contentType_mapping'] = " | ".join(merge_contentType_mapping)
                    self.logger.info('gen merge_contentType_mapping - %s - %s', sid, doc['merge_contentType_mapping'])

                merge_year = jsonutils.get_value_with_default(field_merge_info, "merge_year", list)
                if len(merge_year):
                    doc['merge_year'] = merge_year
                    self.logger.info('gen merge_year - %s - %s', sid, doc['merge_year'])
        except Exception as e:
            self.logger.error('gen field_merge failed - %s', sid)
            self.logger.exception(e)

    def gen_mtags(self, sid, doc):
        try:
            m_tags = []

            tags = jsonutils.get_value_with_default(doc, "tags", list)
            if len(tags):
                m_tags += tags

            area = jsonutils.get_value_with_default(doc, "area", str)
            if area:
                m_tags += [area]

            persons = jsonutils.get_value_with_default(doc, "persons.personName", list)
            if len(persons):
                m_tags += persons

            language = jsonutils.get_value_with_default(doc, "language", str)
            if language:
                m_tags += [language]

            contentType_mapping = jsonutils.get_value_with_default(doc, "merge_contentType_mapping", str)
            if contentType_mapping:
                m_tags += contentType_mapping.strip().split(" | ")

            year = jsonutils.get_value_with_default(doc, "year", int)
            year = str(year).strip()
            if year:
                m_tags += [year]

            m_tags = list(set(m_tags))
            doc["m_tags"] = " | ".join(m_tags)
            self.logger.info('gen mtags - %s - %s', sid, doc["m_tags"])
        except Exception as e:
            self.logger.error('gen mtags failed - %s', sid)
            self.logger.exception(e)

    def gen_minfo(self, sid, doc):
        try:
            m_info = []

            brief = jsonutils.get_value_with_default(doc, "brief", str)
            if brief:
                m_info.append(brief)

            information = jsonutils.get_value_with_default(doc, "information", str)
            if information:
                m_info.append(information)

            m_info = " | ".join(list(set(m_info)))
            doc['m_info'] = m_info
            self.logger.info('gen minfo - %s - %s', sid, doc['m_info'])
        except Exception as e:
            self.logger.error('gen minfo failed - %s', sid)
            self.logger.exception(e)

    def gen_mtitle(self, sid, doc):
        try:
            m_title = []

            title = jsonutils.get_value_with_default(doc, "title", str)
            if title and (not title in m_title):
                m_title += [title]

            subtitle = jsonutils.get_value_with_default(doc, "subtitle", str)
            if subtitle and (not subtitle in m_title):
                m_title += [subtitle]

            name = jsonutils.get_value_with_default(doc, "name", str)
            if name and (not name in m_title):
                m_title += [name]

            normalize_title = jsonutils.get_value_with_default(doc, "normalize_title", str)
            if normalize_title and (not normalize_title in m_title):
                m_title += normalize_title.strip().split(" | ")

            if title and (not (title.lower() in m_title)):
                m_title.append(title.lower())

            m_title = list(set(m_title))
            doc['m_title'] = " | ".join(m_title)
            self.logger.info('gen mtitle - %s - %s', sid, doc['m_title'])
        except Exception as e:
            self.logger.error('gen mtitle failed - %s', sid)
            self.logger.exception(e)

    def remove_ghost_movie_tags(self, sid, doc):
        try:
            tags = jsonutils.get_value_with_default(doc, "tags", list)
            if sid in self.ghost_tags_map:
                for t in self.ghost_tags_map[sid]:
                    if t in tags:
                        tags.remove(t)

            doc["tags"] = tags
            self.logger.info('gen ghost_movie_tags - %s - %s', sid, tags)
        except Exception as e:
            self.logger.error('gen ghost_movie_tags failed - %s', sid)
            self.logger.exception(e)

    def gen_douban_data(self, sid, doc):
        try:
            if sid in self.douban_map:
                (tags, douban_comment_cnt, douban_score, douban_hot) = self.douban_map[sid]

                douban_tags = "|".join(tags)
            else:
                douban_tags = ""
                douban_comment_cnt = 0
                douban_score = 0.0
                douban_hot = 0.0

            doc["douban_tags"] = douban_tags
            doc["douban_comment_cnt"] = douban_comment_cnt
            doc["douban_score"] = douban_score
            doc["douban_hot"] = douban_hot
            self.logger.info('gen douban_data - %s - %s', sid, douban_tags, douban_comment_cnt, douban_score, douban_hot)
        except Exception as e:
            self.logger.error('gen douban_data failed - %s', sid)
            self.logger.exception(e)

if __name__ == '__main__':
    faulthandler.enable()

    AlbumHandler(os.path.dirname(__file__)).start()
