#!/bin/env python
#coding=utf-8

import os
import sys
import re
import logging

sys.path.insert(0, os.getcwd())
import chn_processor
import log

class AlbumExtractor(object):
    """ album data extractor """

    def __init__(self, title, contentType):
        self.title = title
        self.contentType = contentType
        self.name = u''
        self.season = u''
        self.season_field = u''
        self.season_num = -1
        self.hit_st = u'None'
        self.name_type_pri = {
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

    def init(self):
        try:
            # xxxN:, xxxN：, xxxN(), xxxN（）
            self.movie_pattern = re.compile(u'(.*\D{2})(\d)[ :\uff1a (（](.*)[)）]?')
            # xxxN
            self.movie_pattern1 = re.compile(u'(.*\D{2})([1-2][0-9]|[1-9])$')
            # xxx:, xxx：, xxx(), xxx（）
            self.movie_pattern2 = re.compile(u'(.*)[:\uff1a(（](.*)[)）]?')
            # 第N季期部
            self.season_pattern = \
                re.compile(u'第([0-9零一二两三四五六七八九十]+)[\u5b63\u90e8\u671f]')
            # xxx第N季期部$
            self.season_pattern_whole = \
                re.compile(u'(.*?)[\(\uff08]?第([0-9零一二两三四五六七八九十]+)[\u5b63\u90e8\u671f][\)\uff09]?$')
            # xxx第N季期部xx
            self.season_pattern_part = \
                re.compile(u'(.*?)[\(\uff08]?第([0-9零一二两三四五六七八九十]+)[\u5b63\u90e8\u671f][\)\uff09]?')
            # NNNN季期部年xxxx
            self.number_pattern_prefix = \
                re.compile(u'^(\d{4,8})[\u5b63\u90e8\u671f\u5e74]?(\D*)$')
            # xxxxNNNN季期部年
            self.number_pattern_suffix = \
                re.compile(u'(.*?)(\d{4,8})[\u5b63\u90e8\u671f\u5e74]?$')
            # [xxx] 《xxx》 【xxx】 <xxxx>
            self.quota_pattern = re.compile(u'(.*?)[<《\[【](.*?)[>》\]】](.*?)')
            # 《xxx》
            self.book_quota_pattern = re.compile(u'(.*?)[《](.*?)[》](.*?)')
            # xxx xxx版
            self.subtitle_pattern = re.compile(u'(.*?)\u7248$')
#            self.full_quota_pattern = re.compile(u'^[<《\[【](.*?)[>》\]】]$')
            # xx之xxx
            self.chn_subtitle_pattern = re.compile(u'(.*?)\u4e4b(.){2}')
            self.full_quota_patterns = [
                                    re.compile(u'^《(.*?)》$'),
                                    re.compile(u'^\[(.*?)\]$'),
#                                    re.compile(u'^【(.*?)】$'),
                                    re.compile(u'^<(.*?)>$'),
                                    ]
#            self.full_quota_patterns[0] = re.compile(u'^《(.*?)》$')
#            self.full_quota_patterns[1] = re.compile(u'^\[(.*?)\]$')
#            self.full_quota_patterns[2] = re.compile(u'^【(.*?)】$')
#            self.full_quota_patterns[3] = re.compile(u'^<(.*?)>$')
            self.quota_spliter = u'\[|\]|《|》|<|>|【|】'
            self.doc_quota_spliter = u'\[|\]|<|>|【|】'
            #self.comma_spliter = u' |:|\uff1a|\||\u00b7'
            self.comma_spliter = u' |:|\uff1a|\|'
        except Exception, e:
            logging.warning('init failed: %s' % str(e))
        return
    
    def parse_full_quota(self, title):
        for pattern in self.full_quota_patterns:
            m = pattern.search(title)
            if m:
                name = m.group(1).title()
                return name
        return None

    def parse_movie_season(self, title):
        name = u""
        season = u""
        m = self.movie_pattern.search(title)
        if m:
            name = m.group(1).title()
            season = m.group(2).title()
            sub_title = m.group(3).title()
            season_field = name + season
            self.hit_st = u"movie_pattren"
        else:
            m = self.movie_pattern1.search(title)
            if m:
                name = m.group(1).title()
                season = m.group(2).title()
                season_field = name + season
                self.hit_st = u"movie_pattren1"
            else:
                m = self.movie_pattern2.search(title)
                if m:
                    name = m.group(1).title()
                    season_field = m.group(2).title()
                    season_m = self.season_pattern.search(season_field)
                    if season_m:
                        season = season_m.group(1).title()
                    elif season_field.isnumeric():
                        season = season_field
                    else:
                        pass
                    self.hit_st = u"movie_pattern2"
        self.name = name
        self.season = season
        return (name, season)
        
    def parse_quota_fields(self, title):
        name = u""
        season = u""
        m = self.quota_pattern.findall(title)
        subtitles = []
        main_title = u""
        if m:
            for item in m:
                subtitles.append(item[1])
        m = self.book_quota_pattern.search(title)
        if m:
            main_title = m.group(2).title()
        # doc not split
        title_len = len(title)
        main_title_len = len(main_title)
        main_ratio = float(main_title_len) / float(title_len)
        if self.contentType == u"doc" and len(title) > 10 and main_ratio < 0.3:
            fields = re.split(self.doc_quota_spliter, title)
        else:
            fields = re.split(self.quota_spliter, title)

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
                    if (field == main_title):
                        # main title
                        (name, season) = self.parse_comma_fields(field, title)
                        fields_data[f_index]["season"] = season
                        if field == main_title:
                            fields_data[f_index]["type"] = u"main_title"
                        if name:
                            self.hit_st = u"split by quota"
                            fields_data[f_index]["name"] = name
                        else:
                            self.hit_st = u"split by quota comma main_title"
                            fields_data[f_index]["name"] = field
                    elif field.isnumeric():
                        season = field
                        fields_data[f_index]["season"] = season
                        fields_data[f_index]['type'] = u"season_numeric"
                        fields_data[f_index]['name'] = field
                        self.hit_st = u"quota field numeric"
                    elif field not in subtitles:
                        (name, season) = self.parse_comma_fields(field, title)
                        fields_data[f_index]["season"] = season
                        fields_data[f_index]['type'] = u"main_title"
                        if name:
                            self.hit_st = u"split by quota"
                            fields_data[f_index]["name"] = name
                        else:
                            self.hit_st = u"split by quota comma not subtitle"
                            fields_data[f_index]["name"] = field
                        
                    else:
                        m = self.season_pattern.search(field)
                        if m:
                            season = m.group(1).title()
                            self.hit_st = u"split by quota"
                            fields_data[f_index]["season"] = season
                            fields_data[f_index]["type"] = u"season"
                            fields_data[f_index]["name"] = field
                            fields_data[f_index]["type"] = u"subtitle"
                        pass
                else:
                    # blank block, do nothing
                    pass
                f_index += 1
                
            fields_data_sorted = sorted(fields_data, \
                                    key=lambda s:self.name_type_pri[s["type"]], \
                                    reverse=False)
            name = u""
            season = u""
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

#        if not name and not season:
#        if self.contentType in [u'movie', u'tv', u'show']:
#            (name, season) = self.parse_movie_season(title)
        self.name = name.strip()
        self.season = season.strip()
        return
    
    def parse_comma_fields(self, block, title):
        fields = re.split(self.comma_spliter, block)
        # doc not split 
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
            m = self.season_pattern.search(field)
            if m:
                fields_data[f_index]["type"] = u"season"
                season = m.group(1).title()
                fields_data[f_index]["season"] = season
                m_whole = self.season_pattern_whole.search(title)
                if m_whole:
                    name = m_whole.group(1).title()
                    fields_data[f_index]["type"] = u"main_title"
                    fields_data[f_index]["name"] = name
                    self.hit_st = u"season_pattern_whole_title"
                else:
                    m_suffix = self.season_pattern_whole.search(field)
                    if m_suffix:
                        name = m_suffix.group(1).title()
                        self.hit_st = u"season_pattern_whole_field"
                        if name:
                            fields_data[f_index]["type"] = u"season_whole"
                            fields_data[f_index]["name"] = name
                        else:
                            fields_data[f_index]["type"] = u"season_whole"
                            fields_data[f_index]["name"] = u""
                    else:
                        m_part = self.season_pattern_part.search(field)
                        if m_part:
                            self.hit_st = u"season_pattern_part_field"
                            name = m_part.group(1).title()
                            self.hit_st = u"season_pattern_part"
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
                self.hit_st = u"field numeric"
            elif field.encode('utf-8').isalpha():
                fields_data[f_index]['type'] = u"alpha"
                fields_data[f_index]['name'] = field
                self.hit_st = u"field alpha"
            elif not field.isnumeric():
                # prefix number field
                m_num = self.number_pattern_prefix.search(field)
                if m_num:
                    season = m_num.group(1).title()
                    name = m_num.group(2).title()
                    fields_data[f_index]["type"] = u"season_prefix"
                    fields_data[f_index]["name"] = name
                    fields_data[f_index]["season"] = season
                    self.hit_st = u"number_pattern_prefix"
                else:
                    # suffix number field
                    m_num = self.number_pattern_suffix.search(field)
                    if m_num:
                        season = m_num.group(2).title()
                        name = m_num.group(1).title()
                        fields_data[f_index]["name"] = name
                        fields_data[f_index]["season"] = season
                        fields_data[f_index]["type"] = u"season_suffix"
                        self.hit_st = u"number_pattern_suffix"
                    else:
                        m_subtitle = self.subtitle_pattern.search(field)
                        if m_subtitle:
                            fields_data[f_index]["type"] = u"subtitle"
                            fields_data[f_index]["name"] = field
                        m_chn_subtitle = self.chn_subtitle_pattern.search(field)
                        if m_chn_subtitle:
                            name = m_chn_subtitle.group(1).title()
                            if len(name) > 1:
                                fields_data[f_index]["type"] = u"chn_subtitle"
                                fields_data[f_index]["name"] = name
            else:
                pass
            f_index += 1

        logging.debug(fields_data)
        # merge alpha field
        last_alpha = -1
        merge_fields_data = []
        for i in xrange(len(fields_data)):
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

        fields_data_sorted = sorted(merge_fields_data, \
                                    key=lambda s:self.name_type_pri[s["type"]], \
                                    reverse=False)
        logging.debug(fields_data_sorted)
        name = u""
        season = u""
        for item in fields_data_sorted:
            if item["name"] and not name:
                name = item["name"]
            if item["season"] and not season:
                season = item["season"]
        return (name, season)

    def parse_name_season(self, title):
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
        return
    
    def gen_out(self):
        if not self.name:
            self.name = u""
#        output_line = self.title.encode('utf-8') + '\x01' + self.name.encode('utf-8') \
#                + '\x01' + str(self.season_num) + '\x01' + self.hit_st.encode('utf-8')
        output_line = self.title.encode('utf-8') + '\t' + self.name.encode('utf-8') \
                + '\t' + str(self.season_num) + '\t' + self.hit_st.encode('utf-8')
        print output_line

    def get_name(self):
        if not self.name:
            self.name = u""
        return self.name.encode('utf-8')

    def get_season(self):
        return self.season_num

def test():
    for line in sys.stdin:
        line = line.strip()
        (title, contentType) = line.split('\t')
        a = AlbumExtractor(title.decode('utf-8'), contentType.decode('utf-8'))
        a.init()
        a.parse_name_season(a.title)
        a.gen_out()

if __name__ == '__main__':
    log.init_log("logs/album_extract", level=logging.DEBUG)
    #pass
    #a = AlbumExtractor(u'金刚：骷髅岛', u'movie')
    #a = AlbumExtractor(u'【CrystalDreamer】用英语演唱LoveLive的音乐 [3P]', u'movie')
    #a = AlbumExtractor(u'小猪佩奇 第一季 英文版', u'movie')
    #a = AlbumExtractor(u'向往的生活第四季 VIP加长版', u'movie')
    #a = AlbumExtractor(u'偶活学园（偶像活动）Friends! 第二季 普通话版', u'movie')
    #a = AlbumExtractor(u'歌手·当打之年', u'movie')
    # not pass
    #a = AlbumExtractor(u'明星大侦探之名侦探学院 第二季', u'movie')
    #a = AlbumExtractor(u'中餐厅3 会员Plus版', u'movie')
    #a = AlbumExtractor(u'《中餐厅3 会员Plus版》', u'movie')
    #a = AlbumExtractor(u'乡村爱情14', u'movie')
    #a.init()
    #a.parse_name_season(a.title)
    #a.gen_out()
    test()
