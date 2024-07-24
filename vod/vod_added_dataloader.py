#!/usr/bin/env python3

import os
import json
from lib.utils import jsonutils
from . import vod_datadict
import logging

class VodAddedDataloader:

    def __init__(self, logger: logging.Logger, config):
        self.logger = logger

        self.path_series_map = os.path.join(config['root'], config['series_map'])
        self.path_featuretype_map = os.path.join(config['root'], config['feature_type_map'])
        self.path_title_digit_norm_map = os.path.join(config['root'], config['title_digit_norm'])
        self.path_baidu_tags_map = os.path.join(config['root'], config['sid_baidu_tags'])
        self.path_ghost_tags_map = os.path.join(config['root'], config['ghost_film_error_tags'])
        self.path_douban_map = os.path.join(config['root'], config['douban_scores'])
        self.path_virtual_path = os.path.join(config['root'], config['virtual_path'])
        self.path_album_path = os.path.join(config['root'], config['album_path'])

    def get_sereis_map(self):
        series_map = {}

        with open(self.path_series_map) as f:
            for line in f:
                if len(line.strip().split('\t')) != 2:
                    continue

                sid, series_info = line.strip().split('\t')
                series_map[sid] = json.loads(series_info)

        self.logger.info('added_data loaded - series_map - %s', len(series_map))
        return series_map

    def get_featuretype_map(self):
        feature_type_map = {}

        with open(self.path_featuretype_map) as f:
            for line in f:
                if len(line.strip().split('\t')) != 2:
                    continue

                sid, feature_type = line.strip().split('\t')
                feature_type_map[sid] = int(feature_type)

        self.logger.info('added_data loaded - featuretype_map - %s', len(feature_type_map))
        return feature_type_map

    def get_title_digit_norm_map(self):
        title_digit_norm_map = {}

        with open(self.path_title_digit_norm_map, 'r')as f:
            for line in f:
                if len(line.strip().split("\t")) != 2:
                    continue

                sid, titles = line.strip().split("\t")
                title_digit_norm_map[sid] = titles

        self.logger.info('added_data loaded - title_digit_norm_map - %s', len(title_digit_norm_map))
        return title_digit_norm_map

    def get_baidu_tags_map(self):
        baidu_tags_map = {}

        with open(self.path_baidu_tags_map, 'r')as f:
            for line in f:
                if len(line.strip().split("\t")) != 3:
                    continue

                sid, __, baidu_tags = line.strip().split("\t")
                baidu_tags_map[sid] = baidu_tags.strip().split(",")

        self.logger.info('added_data loaded - baidu_tags_map - %s', len(baidu_tags_map))
        return baidu_tags_map

    def get_ghost_tags_map(self):
        ghost_tags_map = {}

        with open(self.path_ghost_tags_map, 'r')as f:
            for line in f:
                if len(line.strip().split("\t")) != 2:
                    continue

                sid, ghost_tags = line.strip().split("\t")
                ghost_tags_map[sid] = ghost_tags.strip().split("|")

        self.logger.info('added_data loaded - ghost_tags_map - %s', len(ghost_tags_map))
        return ghost_tags_map

    def get_douban_map(self):
        douban_map = {}

        with open(self.path_douban_map, 'r') as f:
            for line in f:
                try:
                    line = line.strip()
                    cols = line.split('\t')

                    sid = cols[0]

                    tags_line = cols[2]
                    if tags_line:
                        tags = tags_line.strip().split(',')
                    else:
                        tags = []

                    comment_cnt = cols[3]
                    if not comment_cnt:
                        comment_cnt = 0
                    else:
                        comment_cnt = int(comment_cnt)

                    score = cols[4]
                    if not score:
                        score = 0.0
                    else:
                        score = float(score)

                    hot = cols[5]
                    if not hot:
                        hot = 0.0
                    else:
                        hot = float(hot)

                    douban_map[sid] = (tags, comment_cnt, score, hot)
                except Exception as e:
                    self.logger.error("added_data douban_map failed: %s", line)
                    self.logger.exception(e)

        self.logger.info('added_data loaded - douban_map - %s', len(douban_map))
        return douban_map

    def get_field_merge_map(self):
        vt_sid_dict = self._get_vt_sid_dict()
        album_dict = self._get_album_dict()

        sid_merge_info_dict = {}
        for sid, vt_sid_list in vt_sid_dict.items():
            try:
                sid_album_info = jsonutils.get_value_with_default(album_dict, sid, dict)
                if not len(sid_album_info):
                    continue

                merge_personsName = []
                merge_contentType_mapping = []
                merge_tags = []
                merge_year = []
                merge_area = []
                merge_language = []
                for vt_sid in vt_sid_list:
                    vt_sid_album_info = jsonutils.get_value_with_default(album_dict, vt_sid, dict)
                    if not len(vt_sid_album_info):
                        continue

                    sid_album_persons = jsonutils.get_value_with_default(vt_sid_album_info, "personName", list)
                    if sid_album_persons:
                        merge_personsName += sid_album_persons

                    sid_album_contentType = jsonutils.get_value_with_default(vt_sid_album_info, "contentType", str)
                    contentMapType = jsonutils.get_value_with_default(vod_datadict.contentType_dict, sid_album_contentType, str)
                    if contentMapType.strip():
                        merge_contentType_mapping += [contentMapType]

                    sid_album_tags = jsonutils.get_value_with_default(vt_sid_album_info, "tags", str).strip()
                    if sid_album_tags:
                        merge_tags += sid_album_tags.strip().split("|")

                    sid_album_year = jsonutils.get_value_with_default(vt_sid_album_info, "year", int)
                    if str(sid_album_year).strip():
                        merge_year += [sid_album_year]

                    sid_album_area = jsonutils.get_value_with_default(vt_sid_album_info, "area", str).strip()
                    if sid_album_area:
                        merge_area += sid_album_area.strip().split("|")

                    sid_album_language = jsonutils.get_value_with_default(vt_sid_album_info, "language", str).strip()
                    if sid_album_language:
                        merge_language += sid_album_language.strip().split("|")

                # sort uniq
                sid_merge_info_dict[sid] = {
                    "merge_personsName": list(set(merge_personsName)),
                    "merge_contentType_mapping": list(set(merge_contentType_mapping)),
                    "merge_tags": list(set(merge_tags)),
                    "merge_year": list(set(merge_year)),
                    "merge_area": list(set(merge_area)),
                    "merge_language": list(set(merge_language))
                }
            except Exception as e:
                self.logger.exception(e)

        self.logger.info('added_data loaded - field_merge_map - %s', len(sid_merge_info_dict))
        return sid_merge_info_dict

    def _get_vt_sid_dict(self):
        vt_sid_dict = {}

        with open(self.path_virtual_path, 'r') as fin:
            for line in fin:
                try:
                    data = json.loads(line)
                    vtList = jsonutils.get_value_with_default(data, "virtualProgramRelList", list)

                    sid_list = []
                    for vt in vtList:
                        sid_list.append(jsonutils.get_value_with_default(vt, "sid", str))

                    for vt in vtList:
                        sid = jsonutils.get_value_with_default(vt, "sid", str)

                        if sid in vt_sid_dict:
                            vt_sid_dict[sid] += sid_list
                        else:
                            vt_sid_dict[sid] = sid_list
                except Exception as e:
                    self.logger.exception(e)

        # sort uniq
        for sid, vt_sid_list in vt_sid_dict.items():
            vt_sid_dict[sid] = list(set(vt_sid_list))

        return vt_sid_dict

    def _get_album_dict(self):
        album_dict = {}

        with open(self.path_album_path, 'r') as fin:
            for line in fin:
                try:
                    data = json.loads(line)
                    sid = jsonutils.get_value_with_default(data, "sid", str)

                    personNames = []
                    persons = jsonutils.get_value_with_default(data, "persons", list)
                    if persons is not None:
                        for person in persons:
                            name = jsonutils.get_value_with_default(person, "personName", str)
                            if name:
                                personNames += [name]

                    album_dict[sid] = {
                        "contentType": jsonutils.get_value_with_default(data, "contentType", str),
                        "personName": personNames,
                        "tags": jsonutils.get_value_with_default(data, "tags", list),
                        "area": jsonutils.get_value_with_default(data, "area", str).strip(),
                        "year": jsonutils.get_value_with_default(data, "year", int),
                        "language": jsonutils.get_value_with_default(data, "language", str).strip()
                    }

                    self.logger.info('added_data loaded - album - %s', sid)
                except Exception as e:
                    self.logger.exception(e)

        return album_dict
