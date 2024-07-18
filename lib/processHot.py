#!/usr/bin/env python3

import os
import sys
import json
import time

def process_tencent(data):
    score = 0.0
    if 'score' in data and data['score']:
        score = data['score']

    raw_score = 0.0
    if 'sourceHot' in data and data['sourceHot']:
        raw_score = data['sourceHot']
    elif 'tencent_score' in data and data['tencent_score']:
        raw_score = data['tencent_score']

    if raw_score:
        score = float(raw_score) / 10 * 1.05

        if score > 1:
            score = 1.0
        elif score < 0:
            score = 0.0

    return score

def process_youku(data):
    score = 0.0
    if 'score' in data and data['score']:
        score = data['score']

    if 'sourceHot' in data and data['sourceHot']:
        score = float(data['sourceHot']) / 10000

        if score > 1:
            score = 1.0
        elif score < 0:
            score = 0.0

    return score

def process_iqiyi(data):
    score = 0.0
    if 'score' in data and data['score']:
        score = data['score']

    if 'sourceHot' in data and data['sourceHot']:
        score = float(data['sourceHot']) / 8000

        if score > 1:
            score = 1.0
        elif score < 0:
            score = 0.0

    return score

def process_bilibili(data):
    score = 0.0
    if 'score' in data and data['score']:
        score = data['score']

    if 'playNum' in data and data['playNum'] and 'publishTime' in data and data['publishTime']:
        elapsed_time = time.time() - time.mktime(time.strptime(data['publishTime'], "%Y-%m-%d %H:%M:%S"))
        score = float(data['playNum']) / float(elapsed_time) / 20

        if score > 1:
            score = 1.0
        elif score < 0:
            score = 0.0

    return score

def process_mgtv(data):
    score = 0.0
    if 'score' in data and data['score']:
        score = data['score']

    if 'playNum' in data and data['playNum']:
        score = float(data['playNum']) / 8000

        if score > 1:
            score = 1.0
        elif score < 0:
            score = 0.0

    return score
