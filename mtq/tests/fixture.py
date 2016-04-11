import unittest
import pymongo
import mtq
import logging

log = logging.getLogger('mtq.test')
log.setLevel(logging.INFO)


def test_func(*args, **kwargs):
    log.info('Running test_func')
    return args, kwargs


def test_func_fail(*args, **kwargs):
    log.warning('Raising a test exception in test_func_fail')
    raise Exception()


class MTQTestCase(unittest.TestCase):
    connection = None

    def setUp(self):
        if type(self).connection is None:
            type(self).connection = pymongo.MongoClient()

        self.db = self.connection['mtq_testing']
        self.factory = mtq.create_connection(db=self.db)

    def tearDown(self):
        # uncomment to dump database state after each test
        # from pprint import pprint
        # for coll in self.db.collection_names():
        #     print('\n%s:' % coll)
        #     pprint(list(self.db[coll].find()))
        self.connection.drop_database('mtq_testing')
