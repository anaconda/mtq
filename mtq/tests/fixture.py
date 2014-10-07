import unittest
import pymongo
import mtq


def test_func(*args, **kwargs):
    return args, kwargs

def test_func_fail(*args, **kwargs):
    raise Exception()


class MTQTestCase(unittest.TestCase):
    connection = None
    def setUp(self):
        if type(self).connection is None:
            type(self).connection = pymongo.MongoClient()

        self.db = self.connection['mtq_testing']
        self.factory = mtq.create_connection(db=self.db)

    def tearDown(self):
        self.connection.drop_database('mtq_testing')


