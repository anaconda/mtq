'''
Created on Aug 4, 2013

@author: sean
'''
from mtq.tests.fixture import MTQTestCase

class TestQueue(MTQTestCase):
    
    def test_create_queue(self):
        'Creating queues.'
        q = self.factory.queue('my-queue', ['tags'])
        self.assertEqual(q.name, 'my-queue')
        self.assertEqual(q.count, 0)
        
    def test_enqueue_call(self):
        'Creating queues.'
        q = self.factory.queue('my-queue', ['tags'])
        q.enqueue_call('call-me')
        self.assertEqual(q.count, 1)
