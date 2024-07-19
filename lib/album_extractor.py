#!/usr/bin/env python3

import os
import sys
import re
import chn_processor

name_type_pri = {
    u"main_title": 0,
    u"chn_subtitle": 0,
    u"alpha": 1,
    u"season_whole": 3,
    u"season_part": 3,
    u"season_prefix": 3,
    u"season_suffix": 3,
    u"season": 6,
    u"raw": 6,
    u"season_numeric": 9,
    u"subtitle" : 10,
}

# xxxN:, xxxN：, xxxN(), xxxN（）
movie_pattern = re.compile(u'(.*\D{2})(\d)[ :\uff1a (（](.*)[)）]?')
# xxxN
movie_pattern1 = re.compile(u'(.*\D{2})([1-2][0-9]|[1-9])$')
# xxx:, xxx：, xxx(), xxx（）
movie_pattern2 = re.compile(u'(.*)[:\uff1a(（](.*)[)）]?')
# 第N季期部
season_pattern = re.compile(u'第([0-9零一二两三四五六七八九十]+)[\u5b63\u90e8\u671f]')
# xxx第N季期部$
season_pattern_whole = re.compile(u'(.*?)[\(\uff08]?第([0-9零一二两三四五六七八九十]+)[\u5b63\u90e8\u671f][\)\uff09]?$')
# xxx第N季期部xx
season_pattern_part = re.compile(u'(.*?)[\(\uff08]?第([0-9零一二两三四五六七八九十]+)[\u5b63\u90e8\u671f][\)\uff09]?')
# NNNN季期部年xxxx
number_pattern_prefix = re.compile(u'^(\d{4,8})[\u5b63\u90e8\u671f\u5e74]?(\D*)$')
# xxxxNNNN季期部年
number_pattern_suffix = re.compile(u'(.*?)(\d{4,8})[\u5b63\u90e8\u671f\u5e74]?$')
# [xxx] 《xxx》 【xxx】 <xxxx>
quota_pattern = re.compile(u'(.*?)[<《\[【](.*?)[>》\]】](.*?)')
# 《xxx》
book_quota_pattern = re.compile(u'(.*?)[《](.*?)[》](.*?)')
# xxx xxx版
subtitle_pattern = re.compile(u'(.*?)\u7248$')
# xx之xxx
chn_subtitle_pattern = re.compile(u'(.*?)\u4e4b(.){2}')
full_quota_patterns = [
    re.compile(u'^《(.*?)》$'),
    re.compile(u'^\[(.*?)\]$'),
    re.compile(u'^<(.*?)>$'),
]
quota_spliter = u'\[|\]|《|》|<|>|【|】'
doc_quota_spliter = u'\[|\]|<|>|【|】'
comma_spliter = u' |:|\uff1a|\|'

class AlbumExtractor():
    """ album data extractor """

    def __init__(self, title, contentType):
        self.title = title
        self.contentType = contentType

        self.name = u''
        self.season = u''
        self.season_num = -1

    def get_name(self):
        if not self.name:
            self.name = u""

        return self.name.encode('utf-8')

    def get_season(self):
        return self.season_num

    def parse_name_season(self):
        title = self.title

        # quota
        q_title = self.parse_full_quota(title)
        if q_title:
            title = q_title

            self.parse_movie_season(title)
        else:
            self.parse_quota_fields(title)

        if not self.name:
            self.name = title

        if self.season:
            self.season_num = chn_processor.unicode_to_int(self.season)

    def parse_full_quota(self, title):
        for pattern in full_quota_patterns:
            m = pattern.search(title)
            if m:
                return m.group(1).title()

        return None

    def parse_movie_season(self, title):
        name = u""
        season = u""

        m = movie_pattern.search(title)
        if m:
            name = m.group(1).title()
            season = m.group(2).title()
        else:
            m = movie_pattern1.search(title)
            if m:
                name = m.group(1).title()
                season = m.group(2).title()
            else:
                m = movie_pattern2.search(title)
                if m:
                    name = m.group(1).title()

                    season_field = m.group(2).title()
                    season_m = season_pattern.search(season_field)
                    if season_m:
                        season = season_m.group(1).title()
                    elif season_field.isnumeric():
                        season = season_field

        self.name = name
        self.season = season
        return (name, season)

    def parse_quota_fields(self, title):
        name = u""
        season = u""

        subtitles = []
        m = quota_pattern.findall(title)
        if m:
            for item in m:
                subtitles.append(item[1])

        main_title = u""
        m = book_quota_pattern.search(title)
        if m:
            main_title = m.group(2).title()

        # doc not split
        title_len = len(title)
        main_title_len = len(main_title)
        main_ratio = float(main_title_len) / float(title_len)

        if self.contentType == u"doc" and title_len > 10 and main_ratio < 0.3:
            fields = re.split(doc_quota_spliter, title)
        else:
            fields = re.split(quota_spliter, title)

        for i in fields:
            if not i:
                fields.remove(i)

        fields_data = []
        for i in fields:
            fields_data.append({"raw": i, "type": u"raw", "name": i, "season": u""})

        if len(fields) > 1:
            f_index = 0
            for field in fields:
                if field:
                    if field == main_title:
                        # main title
                        fields_data[f_index]["season"] = season
                        fields_data[f_index]["type"] = u"main_title"

                        (name, season) = self.parse_comma_fields(field, title)
                        if name:
                            fields_data[f_index]["name"] = name
                        else:
                            fields_data[f_index]["name"] = field
                    elif field.isnumeric():
                        season = field

                        fields_data[f_index]["season"] = season
                        fields_data[f_index]['type'] = u"season_numeric"
                        fields_data[f_index]['name'] = field
                    elif field not in subtitles:
                        fields_data[f_index]["season"] = season
                        fields_data[f_index]['type'] = u"main_title"

                        (name, season) = self.parse_comma_fields(field, title)
                        if name:
                            fields_data[f_index]["name"] = name
                        else:
                            fields_data[f_index]["name"] = field
                    else:
                        m = season_pattern.search(field)
                        if m:
                            season = m.group(1).title()

                            fields_data[f_index]["season"] = season
                            fields_data[f_index]["type"] = u"season"
                            fields_data[f_index]["name"] = field
                            fields_data[f_index]["type"] = u"subtitle"

                f_index += 1

            name = u""
            season = u""
            fields_data_sorted = sorted(fields_data, key=lambda s:name_type_pri[s["type"]], reverse=False)
            for item in fields_data_sorted:
                if item["name"] and not name:
                    name = item["name"]
                if item["season"] and not season:
                    season = item["season"]
        elif self.contentType in [u'movie', u'tv', u'show']:
            (name, season) = self.parse_movie_season(title)
        else:
            (name, season) = self.parse_comma_fields(title, title)

        if not name:
            (name, season) = self.parse_comma_fields(title, title)

        self.name = name.strip()
        self.season = season.strip()

    def parse_comma_fields(self, block, title):
        # doc not split
        fields = re.split(comma_spliter, block)
        if fields and self.contentType == u"doc":
            field_len = len(fields[0])
            title_len = len(title)
            field_ratio = float(field_len) / float(title_len)
            if title_len > 10 and field_ratio < 0.3:
                fields = [block]

        fields_data = []
        f_num = len(fields)
        for i in fields:
            fields_data.append({"raw": i, "type": u"raw", "name": i, "season": u""})

        f_index = 0
        for field in fields:
            m = season_pattern.search(field)
            if m:
                fields_data[f_index]["type"] = u"season"
                season = m.group(1).title()
                fields_data[f_index]["season"] = season

                m_whole = season_pattern_whole.search(title)
                if m_whole:
                    name = m_whole.group(1).title()
                    fields_data[f_index]["type"] = u"main_title"
                    fields_data[f_index]["name"] = name
                else:
                    m_suffix = season_pattern_whole.search(field)
                    if m_suffix:
                        name = m_suffix.group(1).title()
                        if name:
                            fields_data[f_index]["type"] = u"season_whole"
                            fields_data[f_index]["name"] = name
                        else:
                            fields_data[f_index]["type"] = u"season_whole"
                            fields_data[f_index]["name"] = u""
                    else:
                        m_part = season_pattern_part.search(field)
                        if m_part:
                            name = m_part.group(1).title()
                            if name:
                                fields_data[f_index]["type"] = u"season_part"
                                fields_data[f_index]["name"] = name
                            else:
                                fields_data[f_index]["type"] = u"season_part"
                                fields_data[f_index]["name"] = u""
            elif field.isnumeric():
                season = field

                fields_data[f_index]["season"] = season
                fields_data[f_index]['type'] = u"season_numeric"
                fields_data[f_index]['name'] = field
            elif field.encode('utf-8').isalpha():
                fields_data[f_index]['type'] = u"alpha"
                fields_data[f_index]['name'] = field
            elif not field.isnumeric():
                # prefix number field
                m_num = number_pattern_prefix.search(field)
                if m_num:
                    season = m_num.group(1).title()
                    name = m_num.group(2).title()

                    fields_data[f_index]["type"] = u"season_prefix"
                    fields_data[f_index]["name"] = name
                    fields_data[f_index]["season"] = season
                else:
                    # suffix number field
                    m_num = number_pattern_suffix.search(field)
                    if m_num:
                        season = m_num.group(2).title()
                        name = m_num.group(1).title()

                        fields_data[f_index]["name"] = name
                        fields_data[f_index]["season"] = season
                        fields_data[f_index]["type"] = u"season_suffix"
                    else:
                        m_subtitle = subtitle_pattern.search(field)
                        if m_subtitle:
                            fields_data[f_index]["type"] = u"subtitle"
                            fields_data[f_index]["name"] = field

                        m_chn_subtitle = chn_subtitle_pattern.search(field)
                        if m_chn_subtitle:
                            name = m_chn_subtitle.group(1).title()
                            if len(name) > 1:
                                fields_data[f_index]["type"] = u"chn_subtitle"
                                fields_data[f_index]["name"] = name

            f_index += 1

        # merge alpha field
        last_alpha = -1
        merge_fields_data = []
        for i in range(len(fields_data)):
            if fields_data[i]["type"] == "alpha" or fields_data[i]["type"] == "season_numeric":
                if last_alpha == -1:
                    last_alpha = i
                    merge_fields_data.append(fields_data[i])
                elif last_alpha == i - 1:
                    temp = merge_fields_data[-1]["name"] + " " + fields_data[i]["name"]
                    merge_fields_data[-1]["name"] = temp
                    last_alpha = i
                else:
                    merge_fields_data.append(fields_data[i])
                    last_alpha = i
            else:
                merge_fields_data.append(fields_data[i])

        name = u""
        season = u""
        fields_data_sorted = sorted(merge_fields_data, key=lambda s:name_type_pri[s["type"]], reverse=False)
        for item in fields_data_sorted:
            if item["name"] and not name:
                name = item["name"]
            if item["season"] and not season:
                season = item["season"]

        return (name, season)
