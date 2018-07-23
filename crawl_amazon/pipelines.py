# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from scrapy.exceptions import DropItem
from datetime import datetime


class FilterPipeline(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        key_id = 'ASIN'

        if key_id not in item:
            id = None
        else:
            id = item[key_id]

        if 'update_variant' not in item:
            if id is None or id in self.ids_seen:
                raise DropItem("Duplicate item found: %s" % item)

        self.ids_seen.add(id)
        return item


class MongoItemsPipeline(object):

    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            mongo_collection=crawler.settings.get('MONGO_ITEMS_COLLECTION')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if 'mongo_collection' in item:
            self.mongo_collection = item['mongo_collection']
            del item['mongo_collection']

        # Add timestamp
        if 'created_at' not in item:
            item['created_at'] = datetime.now()
        item['updated_at'] = datetime.now()

        # Update variant for existed item
        if 'update_variant' in item:
            del item['update_variant']

            parent_id = item['parent_id']
            del item['parent_id']

            self.db[self.mongo_collection].update_one(filter={'ASIN': parent_id},
                                                      update={'$push': {'variants': item}})

            raise DropItem('Update variant {}'.format(item))

        # Create new item
        else:
            print('Insert new item {}'.format(item))

            self.db[self.mongo_collection].insert_one(dict(item))

            return item
