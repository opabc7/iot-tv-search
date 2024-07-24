#!/usr/bin/env python3

mappings = {
    "settings": {
        "index": {
            "number_of_shards": 4,
            "number_of_replicas": 2,
        }
    },
    "mappings": {
        "properties": {
            "area": {
                "type": "keyword",
                "index": True
            },
            "brief": {
                "type": "text",
                "index": True
            },
            "completed": {
                "type": "integer",
                "index": True
            },
            "contentType": {
                "type": "keyword",
                "index": True
            },
            "copyrightCode": {
                "type": "keyword",
                "index": True
            },
            "createTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "duration": {
                "type": "integer",
                "index": True
            },
            "episode": {
                "type": "integer",
                "index": True
            },
            "episodeCount": {
                "type": "integer",
                "index": True
            },
            "episodeUnit": {
                "type": "integer",
                "index": True
            },
            "featureType": {
                "type": "keyword",
                "index": True
            },
	        #(体型) 00-普通模式 01-沉浸式模式
            "experienceType": {
                "type": "keyword",
                "index": True
            },
	        #沉浸式增加背景大图
            "programBackgroundImage": {
                "type": "keyword",
                "index": True
            },
	        #沉浸式增加剧名大图
            "programNameImage": {
                "type": "keyword",
                "index": True
            },
            "episodes.brief": {
                "type": "text",
                "index": True
            },
            "episodes.copyrightCode": {
                "type": "keyword",
                "index": True
            },
            "episodes.createTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "episodes.eid": {
                "type": "keyword",
                "index": True
            },
            "episodes.episode": {
                "type": "integer",
                "index": True
            },
            "episodes.episodeIndex": {
                "type": "integer",
                "index": True
            },
            "episodes.featureType": {
                "type": "integer",
                "index": True
            },
            "episodes.horizontalIcon": {
                "type": "keyword",
                "index": True
            },
            "episodes.linkType": {
                "type": "integer",
                "index": True
            },
            "episodes.linkValue": {
                "type": "keyword",
                "index": True
            },
            "episodes.markCode": {
                "type": "keyword",
                "index": True
            },
            "episodes.productCode": {
                "type": "keyword",
                "index": True
            },
            "episodes.productName": {
                "type": "keyword",
                "index": True
            },
            "episodes.status": {
                "type": "integer",
                "index": True
            },
            "episodes.subscriptCode": {
                "type": "keyword",
                "index": True
            },
            "episodes.title": {
                "type": "text",
                "index": True,
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_smart"
            },
            "episodes.updateTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "episodes.verticalIcon": {
                "type": "keyword",
                "index": True
            },
            "episodes.vipType": {
                "type": "keyword",
                "index": True
            },
            "honor": {
                "type": "keyword",
                "index": True
            },
            "horizontalIcon": {
                "type": "keyword",
                "index": True
            },
            "horizontalImage": {
                "type": "keyword",
                "index": True
            },
            "information": {
                "type": "text",
                "index": True,
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_smart"
            },
            "language": {
                "type": "keyword",
                "index": True
            },
            "linkType": {
                "type": "integer",
                "index": True
            },
            "linkValue": {
                "type": "keyword",
                "index": True
            },
            "markCode": {
                "type": "keyword",
                "index": True
            },
            "period": {
                "type": "keyword",
                "index": True
            },
            # 演员
            "stars": {
                "type": "keyword",
                "index": True
            },
            # 导演
            "directors": {
                "type": "keyword",
                "index": True
            },
            "persons.headImg": {
                "type": "keyword",
                "index": True
            },
            "persons.personName": {
                "type": "text",
                "index": True
            },
            "persons.personSid": {
                "type": "keyword",
                "index": True
            },
            "persons.roleImg": {
                "type": "keyword",
                "index": True
            },
            "persons.roleName": {
                "type": "keyword",
                "index": True
            },
            "persons.roleType": {
                "type": "integer",
                "index": True
            },
            "persons.source": {
                "type": "keyword",
                "index": True
            },
            "persons.squareImg": {
                "type": "keyword",
                "index": True
            },
            "productCode": {
                "type": "keyword",
                "index": True
            },
            "productName": {
                "type": "keyword",
                "index": True
            },
            "programInfo": {
                "type": "keyword",
                "index": True
            },
            "score": {
                "type": "float",
                "index": True
            },
            "sourceScore": {
                "type": "float",
                "index": True
            },
            "showTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "sid": {
                "type": "keyword",
                "index": True
            },
            "source": {
                "type": "keyword",
                "index": True
            },
            "sourceAlbumId": {
                "type": "keyword",
                "index": True
            },
            "cid": {
                "type": "keyword",
                "index": True
            },
            "status": {
                "type": "keyword",
                "index": True
            },
            "subTitle": {
                "type": "text",
                "index": True
            },
            "tags": {
                "type": "keyword",
                "index": True
            },
            "title": {
                "type": "text",
                "index": True,
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_smart"
            },
            "updateTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "verticalIcon": {
                "type": "keyword",
                "index": True
            },
            "verticalImage": {
                "type": "keyword",
                "index": True
            },
            "vipType": {
                "type": "integer",
                "index": True
            },
            "year": {
                "type": "integer",
                "index": True
            },
            "hot":{
                "type": "float",
                "index": True
            },
            "feed_tag":{
                "type": "keyword",
                "index": True
            },
            "virtualSid":{
                "type": "keyword",
                "index": True
            },
            "name":{
                "type": "keyword",
                "index": True
            },
            "season":{
                "type": "integer",
                "index": True
            },
            "series_name":{
                "type": "keyword",
                "index": True
            },
            "series_rank":{
                "type": "integer",
                "index": True
            },
            "m_field":{
                "type": "text",
                "index": True,
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_smart"
            },
            "normalize_title": {
                "type": "text",
                "index": True
            },
            "merge_tags": {
                "type": "text",
                "index": True
            },
            "merge_area": {
                "type": "text",
                "index": True
            },
            "merge_language": {
                "type": "text",
                "index": True
            },
            "merge_contentType_mapping": {
                "type": "text",
                "index": True
            },
            "merge_year": {
                "type": "integer",
                "index": True
            },
            "baidu_tags": {
                "type": "text",
                "index": True
            },
            "m_tags": {
                "type": "text",
                "index": True
            },
            "m_info": {
                "type": "text",
                "index": True
            },
            "m_title": {
                "type": "text",
                "index": True
            },
            "merge_personsName": {
                "type": "text",
                "index": True
            },
            "douban_score": {
                "type": "float",
                "index": True
            },
            "douban_comment_cnt": {
                "type": "integer",
                "index": True
            },
            "douban_hot": {
                "type": "float",
                "index": True
            },
            "douban_tags": {
                "type": "text",
                "index": True
            }
        }
    }
}