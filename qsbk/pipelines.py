# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json

# class QsbkPipeline(object):
#
#     def __init__(self):
#         self.fp = open("duanzi.json", 'w', encoding='utf-8')
#
#     def open_spider(self, spider):
#         print('爬虫开始了。。。')
#
#     def process_item(self, item, spider):
#         item_json = json.dumps(dict(item), ensure_ascii=False)
#         self.fp.write(item_json+'\n')
#         return item
#
#     def close_spider(self, spider):
#         self.fp.close()
#         print('爬虫结束了。。。')
from scrapy.exporters import JsonLinesItemExporter
import pymongo
from twisted.enterprise import adbapi


class QsbkPipeline(object):

    def __init__(self):
        self.fp = open("duanzi.json", 'wb')
        self.exporter = JsonLinesItemExporter(self.fp, ensure_ascii=False, encoding='utf-8')

    def open_spider(self, spider):
        print('爬虫开始了。。。')

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def close_spider(self, spider):
        self.fp.close()
        print('爬虫结束了。。。')


# 同步存储到Mongodb
class SaveMongodbPipeline(object):

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri = crawler.settings.get("MONGO_URI"),
            mongo_db = crawler.settings.get("MONGO_DB")
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        name = item.__class__.__name__
        self.db[name].insert_one(dict(item))
        return item

    def close_spider(self, spider):
        self.client.close()


# 异步存储到Mysql
class SaveMysqlPipeline(object):

    def __init__(self, host, port, db_name, user, password):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host = crawler.settings.get("MYSQL_HOST"),
            port = crawler.settings.get("MYSQL_PORT"),
            db_name = crawler.settings.get("MYSQL_DB_NAME"),
            user = crawler.settings.get("MYSQL_USER"),
            password = crawler.settings.get("MYSQL_PASSWORD")
        )

    def open_spider(self, spider):
        self.dbpool = adbapi.ConnectionPool('pymysql', host=self.host, port=self.port, user=self.user, passwd=self.password, db=self.db_name, charset='utf8')

    def process_item(self, item, spider):
        self.dbpool.runInteraction(self.insert_db, item)

    def close_spider(self, spider):
        self.dbpool.close()

    def insert_db(self, tx, item):
        values = (
            item['author'],
            item['stats_vote'],
            item['content']
        )
        sql = 'INSERT INTO qsbk (author, stats_vote, content) VALUES (%s, %s, %s)'
        tx.execute(sql, values)