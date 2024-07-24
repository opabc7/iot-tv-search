#!/usr/bin/env python3

mappings = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
        }
    },
    "mappings": {
        "properties": {
            "sid": {
                "type": "keyword",
                "index": True
            },
            "backImg": {
                "type": "keyword",
                "index": True
            },
            "birthday": {
                "type": "keyword",
                "index": True
            },
            "birthplace": {
                "type": "keyword",
                "index": True
            },
            "bloodType": {
                "type": "keyword",
                "index": True
            },
            "bodyImg": {
                "type": "keyword",
                "index": True
            },
            "cnAlias": {
                "type": "keyword",
                "index": True
            },
            "cnInitial": {
                "type": "keyword",
                "index": True
            },
            "cnName": {
                "type": "keyword",
                "index": True
            },
            "cnPinyin": {
                "type": "keyword",
                "index": True
            },
            "constellation": {
                "type": "keyword",
                "index": True
            },
            "createTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "enAlias": {
                "type": "keyword",
                "index": True
            },
            "foreName": {
                "type": "keyword",
                "index": True
            },
            "headImg": {
                "type": "keyword",
                "index": True
            },
            "height": {
                "type": "keyword",
                "index": True
            },
            "hobby": {
                "type": "keyword",
                "index": True
            },
            "name": {
                "type": "keyword",
                "index": True,
            },
            "nation": {
                "type": "keyword",
                "index": True
            },
            "occupation": {
                "type": "keyword",
                "index": True
            },
            "sex": {
                "type": "keyword",
                "index": True
            },
            "source": {
                "type": "keyword",
                "index": True
            },
            "squareImg": {
                "type": "keyword",
                "index": True
            },
            "status": {
                "type": "integer",
                "index": True
            },
            "summary": {
                "type": "text",
                "index": True,
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_smart"
            },
            "occupation": {
                "type": "keyword",
                "index": True
            },
            "updateTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "weiboId": {
                "type": "keyword",
                "index": True
            },
            "posters.createTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "posters.personSid": {
                "type": "keyword",
                "index": True
            },
            "posters.poster": {
                "type": "keyword",
                "index": True
            },
            "posters.posterKey": {
                "type": "keyword",
                "index": True
            },
            "posters.status": {
                "type": "integer",
                "index": True
            },
            "posters.updateTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "awards.createTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "awards.award": {
                "type": "keyword",
                "index": True
            },
            "awards.awardKey": {
                "type": "keyword",
                "index": True
            },
            "awards.awardNumber": {
                "type": "keyword",
                "index": True
            },
            "awards.awardPerson": {
                "type": "keyword",
                "index": True
            },
            "awards.awardProgram": {
                "type": "keyword",
                "index": True
            },
            "awards.awardYear": {
                "type": "keyword",
                "index": True
            },
            "awards.personSid": {
                "type": "keyword",
                "index": True
            },
            "awards.rewarded": {
                "type": "keyword",
                "index": True
            },
            "awards.subAward": {
                "type": "keyword",
                "index": True
            },
            "awards.status": {
                "type": "integer",
                "index": True
            },
            "awards.updateTime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                "index": True
            },
            "works": {
                "type": "nested"
            }
        }
    }
}