#!/usr/bin/env python3

import faulthandler
import os
from vod.handler import VodHandler
from lib.rocksclient import RocksClient
from elasticsearch import Elasticsearch
from mappings import vod_person_mapping

class PersonHandler(VodHandler):

    def __init__(self, work_dir):
        self.task = os.environ['vod_task'] = 'person'

        VodHandler.__init__(self, work_dir, 'sid', 'title')

        self.rocksclient = RocksClient(self.rocksdb_path, 'rw')

        self.es = Elasticsearch(self.es_hosts)
        if self.es.indices.exists(index = self.index_name):
            self.logger.info("search index %s has already existed.", self.index_name)
        else:
            es_res = self.es.indices.create(index = self.index_name, mappings = vod_person_mapping.mappings)
            self.logger.info("search index created, result: %s", es_res)

    def init_config_task(self):
        task_config = VodHandler.init_config_task(self)

        # config:task:es
        self.index_name = task_config['es']['index_name']

        # config:task:rocksdb
        self.rocksdb_path = os.path.join(self.rocksdb_config['root'], self.rocksdb_config[self.task])

    def process_doc(self, doc):
        doc_plus = doc.copy()

        doc_plus['posters.createTime'] = []
        doc_plus['posters.personSid'] = []
        doc_plus['posters.poster'] = []
        doc_plus['posters.posterKey'] = []
        doc_plus['posters.status'] = []
        doc_plus['posters.updateTime'] = []
        if 'poster' in doc_plus:
            for poster in doc_plus['poster']:
                if 'createTime' in poster:
                    doc_plus['posters.createTime'].append(poster["createTime"])
                if 'personSid' in poster:
                    doc_plus['posters.personSid'].append(poster["personSid"])
                if 'poster' in poster:
                    doc_plus['posters.poster'].append(poster["poster"])
                if 'posterKey' in poster:
                    doc_plus['posters.posterKey'].append(poster["posterKey"])
                if 'status' in poster:
                    doc_plus['posters.status'].append(poster["status"])
                if 'updateTime' in poster:
                    doc_plus['posters.updateTime'].append(poster["updateTime"])

        doc_plus['awards.award'] = []
        doc_plus['awards.awardKey'] = []
        doc_plus['awards.awardNumber'] = []
        doc_plus['awards.awardPerson'] = []
        doc_plus['awards.awardProgram'] = []
        doc_plus['awards.awardYear'] = []
        doc_plus['awards.createTime'] = []
        doc_plus['awards.personSid'] = []
        doc_plus['awards.rewarded'] = []
        doc_plus['awards.status'] = []
        doc_plus['awards.subAward'] = []
        doc_plus['awards.updateTime'] = []
        if 'awards' in doc_plus:
            for award in doc_plus['awards']:
                if 'award' in award:
                    doc_plus['awards.award'].append(award['award'])
                if 'awardKey' in award:
                    doc_plus['awards.awardKey'].append(award['awardKey'])
                if 'awardNumber' in award:
                    doc_plus['awards.awardNumber'].append(award['awardNumber'])
                if 'awardPerson' in award:
                    doc_plus['awards.awardPerson'].append(award['awardPerson'])
                if 'awardProgram' in award:
                    doc_plus['awards.awardProgram'].append(award['awardProgram'])
                if 'awardYear' in award:
                    doc_plus['awards.awardYear'].append(award['awardYear'])
                if 'createTime' in award:
                    doc_plus['awards.createTime'].append(award['createTime'])
                if 'personSid' in award:
                    doc_plus['awards.personSid'].append(award['personSid'])
                if 'rewarded' in award:
                    doc_plus['awards.rewarded'].append(award['rewarded'])
                if 'status' in award:
                    doc_plus['awards.status'].append(award['status'])
                if 'subAward' in award:
                    doc_plus['awards.subAward'].append(award['subAward'])
                if 'updateTime' in award:
                    doc_plus['awards.updateTime'].append(award['updateTime'])

        return doc_plus

    def write_plus(self, _id, title, body_plus, doc_plus):
        self.rocksclient.put(_id, body_plus)
        self.logger.info('rocks put - %s', body_plus)

        try:
            es_res = self.es.index(index = self.index_name, id = _id, document = doc_plus)
            self.logger.info("search update succeded: %s", es_res)
        except Exception as e:
            self.logger.error("search update failed: %s", body_plus)
            self.logger.exception(e)

if __name__ == '__main__':
    faulthandler.enable()

    PersonHandler(os.path.dirname(__file__)).start()
