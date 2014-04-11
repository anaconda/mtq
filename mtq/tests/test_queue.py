'''
Created on Aug 4, 2013

@author: sean
'''
from mtq.tests.fixture import MTQTestCase
import mtq.tests.fixture
from mtq.utils import now
import unittest
from mtq.queue import QueueError
import time

class TestQueue(MTQTestCase):

    def test_create_queue(self):
        'Creating queues.'
        q = self.factory.queue('my-queue', ['tags'])
        self.assertEqual(q.name, 'my-queue')
        self.assertEqual(q.count, 0)

        self.assertEqual(str(q), 'my-queue')
        self.assertEqual(repr(q), "<mtq.Queue name:my-queue tags:('tags',)>")

    def test_enqueue_call(self):
        'Creating queues.'
        q = self.factory.queue('my-queue', ['tags'])
        q.enqueue_call('call-me')
        self.assertEqual(q.count, 1)

        job = q.enqueue_call(mtq.tests.fixture.test_func, args=(1,))
        self.assertEqual(job.func_name, 'mtq.tests.fixture.test_func')
        self.assertEqual(q.count, 2)

        job = q.enqueue_call(mtq.tests.fixture.test_func, args=(1,), kwargs={'this':1})
        self.assertEqual(job.func_name, 'mtq.tests.fixture.test_func')
        self.assertEqual(q.count, 3)

    def test_enqueue_call_error(self):
        'Creating queues.'
        q = self.factory.queue('my-queue', ['tags'])

        with self.assertRaises(QueueError):
            q.enqueue_call(object())

        with self.assertRaises(TypeError):
            q.enqueue_call('fine', args='str')

        with self.assertRaises(TypeError):
            q.enqueue_call('fine', kwargs='str')


    def test_enqueue(self):
        'Creating queues.'
        q = self.factory.queue('my-queue', ['tags'])

        q.enqueue('call-me')


    def test_jobs(self):
        'Creating queues.'
        q = self.factory.queue('my-queue', ['tags'])
        q.enqueue_call('call-me')
        self.assertEqual(len(q.jobs), 1)
        job = q.jobs[0]
        self.assertEqual(job.tags, ('tags',))
        self.assertEqual(job.func_name, 'call-me')
        self.assertEqual(job.qname, 'my-queue')

        job.cancel()
        self.assertEqual(len(q.jobs), 0)

    def test_is_empty(self):
        """Detecting empty queues."""
        q = self.factory.queue('example')
        self.assertEqual(q.is_empty(), True)

        self.factory.queue_collection.insert({'qname':'example',
                                              'process_after': now(),
                                              'priority': 0,
                                              'processed':False
                                              })
        self.assertEqual(q.is_empty(), False)

    def test_all_tags(self):

        q = self.factory.queue('my-queue')
        q.enqueue_call('call-me', tags=['1', '2'])
        q.enqueue_call('call-me', tags=['2', '3'])

        self.assertEqual(q.count, 2)

        self.assertEqual(q.all_tags, set(['1', '2', '3']))

    def test_tag_count(self):

        q = self.factory.queue('my-queue')
        q.enqueue_call('call-me1', tags=['1', '2'])
        q.enqueue_call('call-me2', tags=['2', '3'])

        self.assertEqual(q.tag_count('1'), 1)
        self.assertEqual(q.tag_count('2'), 2)
        self.assertEqual(q.tag_count('3'), 1)


    def test_pop(self):
        q = self.factory.queue('my-queue')
        q.enqueue_call('call-me1', tags=['1', '2'])

        job = q.pop('worker')
        self.assertIsNotNone(job)
        self.assertEqual(job.func_name, 'call-me1')

        job = q.pop('worker')
        self.assertIsNone(job)

    def test_mutex1(self):
        q = self.factory.queue('my-queue')
        q.enqueue_call('call-me1', mutex={'key': 'key1', 'count': 1})
        q.enqueue_call('call-me2', mutex={'key': 'key1', 'count': 1})
        q.enqueue_call('call-me3', mutex={'key': 'key2', 'count': 1})

        job1 = q.pop('worker')
        job2 = q.pop('worker')
        job3 = q.pop('worker')

        self.assertIsNotNone(job1)
        self.assertIsNotNone(job2)
        self.assertIsNone(job3)

        job1.set_finished()
        job2.set_finished()

        job3 = q.pop('worker')

        self.assertIsNotNone(job3)

    def test_mutex2(self):

        q = self.factory.queue('my-queue')
        q.enqueue_call('call-me1', mutex={'key': 'key1', 'count': 2})
        q.enqueue_call('call-me2', mutex={'key': 'key1', 'count': 2})
        q.enqueue_call('call-me3', mutex={'key': 'key1', 'count': 2})
        q.enqueue_call('call-me4', mutex={'key': 'key2', 'count': 1})

        job1 = q.pop('worker')
        job2 = q.pop('worker')
        job3 = q.pop('worker')
        job4 = q.pop('worker')

        self.assertIsNotNone(job1)
        self.assertIsNotNone(job2)
        self.assertIsNotNone(job3)
        self.assertIsNone(job4)



if __name__ == '__main__':
    unittest.main()
