# This function provides a database agnostic set of calls to a database

import pymongo
from pymongo import MongoClient
from pprint import pprint
import datetime

import unittest
import sys


class database(object):
    mongoClient = None  # Connected to database
    mongoDb = None  # Current database
    mongoCol = None
    cursor = None  # Current data cursor

    def __init__(self, database='visualsearch', port=27017,
                 client="192.168.86.192"):
        """
        Constructor
        """

        self.mongoClient = MongoClient(client, port)

        if not self.mongoClient:
            print("Cannot connect to database")

        if database:
            self.setDb(database)

    def setDb(self, database):
        self.mongoDb = self.mongoClient[database]

    def setCollection(self, collection):
        self.mongoCol = self.mongoDb[collection]

    def createIndex(self, index, unique=False, collection=None):

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        col.create_index(index, unique=unique)

    def insert(self, record, collection=None, addTimestamp=False):
        """
        Insert new row into a collection
        """

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return False

        if addTimestamp:
            record['timestamp'] = datetime.datetime.utcnow()

        try:
            result = col.insert_one(record)
        except:  # Exception, e:
            print("error database.insert: ")
            return False

        return True

    def upsert(self, record, uid, collection=None, updateTimestamp=True,
               originalTimestamp=False):
        """
        If existing record exists, update it.  If not, insert a new record
        """

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        if originalTimestamp:
            record['timestamp'] = originalTimestamp
        elif updateTimestamp:
            record['timestamp'] = datetime.datetime.utcnow()

        try:
            result = col.update(
                {"_id": uid},
                record,
                upsert=True
            )
        except:  # Exception, e:
            print("error database.upsert: ")
            return False

        return result

    def update(self, newFields, match, collection=None):

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        query.append({"$set": newFields})

        try:
            result = col.update_one(match)
        except:
            print("update error")
            return False

        return result

    def delete(self, query="{}", collection=None):

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        try:
            result = col.delete_many(query)
        except:
            print("delete error")
            return False

        return result

    def deleteAll(self, collection=None):

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        col.remove({})

    def cursorSort(self, field, collection=None):
        if self.cursor:
            self.cursor.sort(field, pymongo.DESCENDING)

    def searchCount(self, query="", collection=None):

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        if len(query):
            return col.find(query, no_cursor_timeout=True).count()
        else:
            return col.find(no_cursor_timeout=True).count()

    def findOne(self, query="", collection=None):
        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        result = col.find_one(query, no_cursor_timeout=True)
        return result

    def sample(self, collection=None):

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        return list(col.aggregate([{"$sample": {"size": 1}}]))

    def search(self, query="", collection=None, sortArgs=None):

        if collection:
            col = self.mongoDb[collection]
        elif self.mongoCol:
            col = self.mongoCol
        else:
            raise ValueError('No collection specified')
            return

        if len(query) > 0:
            if sortArgs:
                self.cursor = col.find(query, no_cursor_timeout=True).sort(
                    sortArgs)
            else:
                self.cursor = col.find(query, no_cursor_timeout=True)
        else:
            self.cursor = col.find(no_cursor_timeout=True)

        return self.cursor

    def cursorGetOne(self, cursor=None):

        if cursor:
            cur = cursor
        elif self.cursor:
            cur = self.cursor
        else:
            raise ValueError('No cursor specified')
            return

        try:
            val = self.cursor.next()
            return val
        except StopIteration:
            return None

    def cursorGet(self, cursor=None):
        if cursor:
            cur = cursor
        elif self.cursor:
            cur = self.cursor
        else:
            raise ValueError('No cursor specified')
            return

        return self.cursor

    def cursorClose(self, cursor=None):
        if cursor:
            cur = cursor
        elif self.cursor:
            cur = self.cursor
        else:
            raise ValueError('No cursor specified')
            return

        self.cursor.close()

    def cursorGetCount(self, cursor=None):
        if cursor:
            cur = cursor
        elif self.cursor:
            cur = self.cursor
        else:
            raise ValueError('No cursor specified')
            return

        return self.cursor.count()


class Test(unittest.TestCase):

    def testSearch(self):

        print("Testing database")

        # initializing database

        print("Connecting to DB")
        mongo = database("testing_db")
        mongo.setColletion('testing_collection')

        print("Deleting all existing records")
        res = mongo.delete({})

        print("Upserting first record")
        res = mongo.upsert({"name": "dan", "species": "vulcan"}, "12345")
        mongo.search()
        mongo.cursorSort([('name', -1)])
        res = mongo.cursorGet()
        for a in res:
            pprint(a)

        print("Updating first record")
        res = mongo.upsert({"name": "dan", "species": "earthling"}, "12345")
        print(mongo.searchCount({"name": "dan"}))
        res = mongo.search()
        for a in res:
            pprint(a)

        print("Upserting second record")
        res = mongo.upsert({"name": "jim", "species": "earthling"}, "12347")
        res = mongo.search()
        for a in res:
            pprint(a)

        print("Deleting dans")
        res = mongo.delete({"name": "dan"})
        res = mongo.search()
        for a in res:
            pprint(a)

        print("Upserting second record")
        res = mongo.upsert({"name": "jim", "species": "earthling"}, "12346")
        res = mongo.search()
        for a in res:
            pprint(a)

        print("Deleting all existing records")
        res = mongo.delete({})
        res = mongo.search()
        for a in res:
            pprint(a)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()