#!/usr/bin/env python3

import json
from ..lib.utils import jsonutils
import vod_datadict

def get_sereis_map(fpath):
    series_map = {}

    with open(fpath) as f:
        for line in f:
            if len(line.strip().split('\t')) != 2:
                continue

            sid, series_info = line.strip().split('\t')
            series_map[sid] = json.loads(series_info)

    return series_map

def get_featuretype_map(fpath):
    feature_type_map = {}

    with open(fpath) as f:
        for line in f:
            if len(line.strip().split('\t')) != 2:
                continue

            sid, feature_type = line.strip().split('\t')
            feature_type_map[sid] = int(feature_type)

    return feature_type_map

def get_title_digit_norm_map(fpath):
    title_digit_norm_map = {}

    with open(fpath, 'r')as f:
        for line in f:
            if len(line.strip().split("\t")) != 2:
                continue

            sid, titles = line.strip().split("\t")
            title_digit_norm_map[sid] = titles

    return title_digit_norm_map

def get_baidu_tags_map(fpath):
    baidu_tags_map = {}

    with open(fpath, 'r')as f:
        for line in f:
            if len(line.strip().split("\t")) != 3:
                continue

            sid, __, baidu_tags = line.strip().split("\t")
            baidu_tags_map[sid] = baidu_tags.strip().split(",")

    return baidu_tags_map

def get_ghost_tags_map(fpath):
    ghost_tags_map = {}

    with open(fpath, 'r')as f:
        for line in f:
            if len(line.strip().split("\t")) != 2:
                continue

            sid, ghost_tags = line.strip().split("\t")
            ghost_tags_map[sid] = ghost_tags.strip().split("|")

    return ghost_tags_map

def get_douban_map(fpath):
    douban_map = {}

    with open(fpath, 'r') as f:
        for line in f:
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

    return douban_map

def get_field_merge_map(virtual_path, album_path):
    vt_sid_dict = _get_vt_sid_dict(virtual_path)
    album_dict = _get_album_dict(album_path)

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
            continue

    return sid_merge_info_dict

def _get_vt_sid_dict(virtual_path):
    vt_sid_dict = {}

    with open(virtual_path, 'r') as fin:
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
                continue

    # sort uniq
    for sid, vt_sid_list in vt_sid_dict.items():
        vt_sid_dict[sid] = list(set(vt_sid_list))

    return vt_sid_dict

def _get_album_dict(album_path):
    album_dict = {}

    with open(album_path, 'r') as fin:
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
            except Exception as e:
                continue

    return album_dict
