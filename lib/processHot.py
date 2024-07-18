#!/bin/env python
#coding=utf-8

import os
import sys
import json
import time


key_list = ['score', 'playQuality', 'playNum', 'popularity', 'source', \
        'contentQuality', 'freshness', 'sid', 'publishTime', 'sourceHot', 'tencent_score', 'new_score']

def print_head():
    out = ""
    for key in key_list:
        out += key + '\t'
    #print out

def process_tencent(data):
    score = 0.0
    if 'score' in data:
        score = data['score']
    raw_score = 0.0
    if 'sourceHot' in data and data['sourceHot']:
        raw_score = data['sourceHot']
    elif 'tencent_score' in data and data['tencent_score']:
        raw_score = data['tencent_score']
    if not raw_score:
        return score
    score = float(raw_score) / 10 * 1.05
    if score > 1:
        score = 1.0
    if score < 0:
        score = 0.0
    return score

def process_youku(data):
    score = 0.0
    if 'score' in data:
        score = data['score']
    if 'sourceHot' not in data:
        return score
    raw_score = data['sourceHot']
    if not raw_score:
        return score
    score = float(raw_score) / 10000
    if score > 1:
        score = 1.0
    if score < 0:
        score = 0.0
    return score

def process_iqiyi(data):
    score = 0.0
    if 'score' in data:
        score = data['score']
    if 'sourceHot' not in data:
        return score
    raw_score = data['sourceHot']
    if not raw_score:
        return score
    score = float(raw_score) / 8000
    if score > 1:
        score = 1.0
    if score < 0:
        score = 0.0
    return score

def process_bilibili(data):
    score = 0.0
    if 'score' in data:
        score = data['score']
    if 'playNum' not in data:
        return score
    raw_score = data['playNum']
    if not raw_score:
        return score
    if 'publishTime' not in data:
        return score
    publishTime = data['publishTime']
    formattime = time.strptime(publishTime,"%Y-%m-%d %H:%M:%S")
    now = time.time()
    publish = time.mktime(formattime)
    last = now-publish
    score = float(raw_score) / float(last) / 20
    if score > 1:
        score = 1.0
    if score < 0:
        score = 0.0
    return score

def process_mgtv(data):
    score = 0.0
    if 'score' in data:
        score = data['score']
    if 'playNum' not in data:
        return score
    raw_score = data['playNum']
    if not raw_score:
        return score
    score = float(raw_score) / 8000
    if score > 1:
        score = 1.0
    if score < 0:
        score = 0.0
    return score

def process_data(line):
    try:
        line = line.strip()
        data = json.loads(line)
        if 'source' not in data:
            return
        if data['source'] == "tencent":
            score = process_tencent(data)
        elif data['source'] == "youku":
            score = process_youku(data)
        elif data['source'] == "iqiyi":
            score = process_iqiyi(data)
        elif data['source'] == "bilibili":
            score = process_bilibili(data)
        elif data['source'] == "mgtv":
            score = process_mgtv(data)
        else:
            score = 0

        data['new_score'] = score
        
        out = ''
        for key in key_list:
            if key in data:
                if type(data[key]) in (int, float):
                    out += str(data[key]) + '\t'
                elif data[key] == None:
                    out += "null\t"
                else:
                    out += data[key] + '\t'
            else:
                out += 'null\t'
        #print out
        return score
    except Exception, e:
        print >> sys.stderr, "exp: " + str(e)
        print >> sys.stderr, line

if __name__ == "__main__":
    print_head()
    for line in sys.stdin:
        line = line.strip()
        score = process_data(line)
#        print score
