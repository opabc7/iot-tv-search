#!/usr/bin/env python3

def generate_sereis_map(fpath):
    series_map = {}
    
    with open(fpath) as f:
        for line in f:
            if len(line.strip().split('\t')) != 2:
                continue
            
            sid, series_info = line.strip().split('\t')
            series_info = json.loads(series_info)
            series_map[sid] = series_info

    return series_map

def generate_featuretype_map(fpath):
    feature_type_map = {}
    
    with open(fpath) as f:
        for line in f:
            if len(line.strip().split('\t')) != 2:
                continue
            
            sid, feature_type = line.strip().split('\t')
            feature_type_map[sid] = int(feature_type)

    return feature_type_map

def gen_title_digit_norm_map(fpath):
    title_digit_norm_map = {}
    
    with open(fpath, 'r')as f:
        for line in f:
            if len(line.strip().split("\t")) != 2:
                continue
            
            sid, titles = line.strip().split("\t")
            title_digit_norm_map[sid] = titles

    return title_digit_norm_map

def gen_baidu_tags_map(fpath):
    baidu_tags_map = {}
    
    with open(fpath, 'r')as f:
        for line in f:
            if len(line.strip().split("\t")) != 3:
                continue
            
            sid, title, baidu_tags = line.strip().split("\t")
            baidu_tags = baidu_tags.strip().split(",")
            baidu_tags_map[sid] = baidu_tags

    return baidu_tags_map

def gen_album_sid_info_dict(self):
        with open(self.album_path, 'r')as fin:
            res = {}
            for line in fin:
                try:
                    line = json.loads(line)
                    sid = json_data(line, "sid", str)

                    album_persons = json_data(line, "persons", list)
                    personName = []
                    if not (album_persons is None):
                        for persons in album_persons:
                            Name = json_data(persons, "personName", str)
                            if Name:
                                personName += [Name]
                    contentType = json_data(line, "contentType", str)
                    tags = json_data(line, "tags", list)
                    year = json_data(line, "year", int)
                    area = json_data(line, "area", str).strip()
                    language = json_data(line, "language", str).strip()
                    res = {"contentType": contentType, "personName": personName, "tags": tags, "area": area, "year": year, "language": language}
                    self.album_dict[sid] = res
                except Exception, e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    logging.error("gen_album_sid_info_dict error({}):{}".format(str(exc_tb.tb_lineno), str(e)))
                    continue
        return

    def gen_virtual_dict(self):
        with open(self.virtual_path, 'r')as fin:
            for line in fin:
                try:
                    line = json.loads(line)
                    vtSid = json_data(line, "virtualSid", str)
                    vtList = json_data(line, "virtualProgramRelList", list)
                    sid_list = []
                    for vt in vtList:
                        sid = json_data(vt, "sid", str)
                        sid_list.append(sid)
                    for vt in vtList:
                        sid = json_data(vt, "sid", str)
                        if sid in self.vt_sid_dict:
                            self.vt_sid_dict[sid] += sid_list
                        else:
                            self.vt_sid_dict[sid] = sid_list
                except Exception, e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    logging.error("virtual dict error({}):{}".format(str(exc_tb.tb_lineno), str(e)))
                    continue
        for sid, vt_sid_list in self.vt_sid_dict.items():
            self.vt_sid_dict[sid] = list(set(vt_sid_list))
        return

    def gen_sid_merge_info(self):
        for sid, vt_sid_list in self.vt_sid_dict.items():
            try:
                merge_personsName = []
                merge_contentType_mapping = []
                merge_tags = []
                merge_year = []
                merge_area = []
                merge_language = []
                sid_album_info = json_data(self.album_dict, sid, dict)
                if not len(sid_album_info):
                    continue
                for vt_sid in vt_sid_list:
                    vt_sid_album_info = json_data(self.album_dict, vt_sid, dict)
                    if not len(vt_sid_album_info):
                        continue
                    sid_album_persons = json_data(vt_sid_album_info, "personName", list)
                    if sid_album_persons:
                        merge_personsName += sid_album_persons
                    sid_album_contentType = json_data(vt_sid_album_info, "contentType", str)
                    contentMapType = json_data(self.MAPPING, sid_album_contentType, str)
                    if contentMapType.strip():
                        merge_contentType_mapping += [contentMapType]
                    sid_album_tags = json_data(vt_sid_album_info, "tags", str).strip()
                    if sid_album_tags:
                        sid_album_tags = sid_album_tags.strip().split("|")
                        merge_tags += sid_album_tags
                    sid_album_year = json_data(vt_sid_album_info, "year", int)
                    if str(sid_album_year).strip():
                        merge_year += [sid_album_year]
                    sid_album_area = json_data(vt_sid_album_info, "area", str).strip()
                    if sid_album_area:
                        sid_album_area = sid_album_area.strip().split("|")
                        merge_area += sid_album_area
                    sid_album_language = json_data(vt_sid_album_info, "language", str).strip()
                    if sid_album_language:
                        sid_album_language = sid_album_language.strip().split("|")
                        merge_language += sid_album_language
                merge_contentType_mapping = list(set(merge_contentType_mapping))
                merge_tags = list(set(merge_tags))
                merge_year = list(set(merge_year))
                merge_area = list(set(merge_area))
                merge_language = list(set(merge_language))
                merge_personsName = list(set(merge_personsName))
                self.sid_merge_info_dict[sid] = {"merge_personsName": merge_personsName, \
                        "merge_contentType_mapping": merge_contentType_mapping, "merge_tags": merge_tags, "merge_year": merge_year,\
                        "merge_area": merge_area, "merge_language": merge_language}
            except Exception, e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                logging.error("sid_merge_info error({}):{}".format(str(exc_tb.tb_lineno), str(e)))
                continue
        return

def gen_field_merge_map(fpath):
    self.gen_album_sid_info_dict()
    self.gen_virtual_dict()
    self.gen_sid_merge_info()
    return self.sid_merge_info_dict

def gen_ghost_tags_map(fpath):
    ghost_tags_map = {}
    
    with open(fpath, 'r')as f:
        for line in f:
            if len(line.strip().split("\t")) != 2:
                continue
            
            sid, ghost_tags = line.strip().split("\t")
            ghost_tags = ghost_tags.strip().split("|")
            ghost_tags_map[sid] = ghost_tags

    return ghost_tags_map

def gen_douban_map(fpath):
    douban_map = {}
    
    with open(fpath, 'r') as f:
        for line in f:
            line = line.strip()
            cols = line.split('\t')
            sid = cols[0]
            title = cols[1]
            tags_line = cols[2]
            tags = []
            if not tags_line:
                tags_line = ""
            else:
                tags = tags_line.strip().split(',')
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
