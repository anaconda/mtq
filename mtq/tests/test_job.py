'''
Created on Aug 5, 2013

@author: sean
'''

from mtq.tests.fixture import MTQTestCase
import mtq.tests.fixture
from mtq.utils import now
import unittest
from mtq.queue import QueueError

class TestQueue(MTQTestCase):

    def test_create_job(self):
        'Creating jobs.'
        q = self.factory.queue('my-queue', ['tags'])
        q.enqueue(mtq.tests.fixture.test_func, 1, 2, a=1, b=2)
        job = q.pop('worker')

        self.assertEqual(job.func_name, 'mtq.tests.fixture.test_func')

        job = self.factory.get_job(job.id)

        self.assertEqual(job.args, [1, 2])
        self.assertEqual(job.kwargs, {'a':1, 'b':2})
        self.assertEqual(job.func, mtq.tests.fixture.test_func)

        args, kwargs = job.apply()
        self.assertEqual(args, (1, 2))
        self.assertEqual(kwargs, {'a':1, 'b':2})

        self.assertEqual(job.tags, ['tags'])

    def test_job_finished(self):
        'Creating jobs.'
        q = self.factory.queue('my-queue', ['tags'])
        job = q.enqueue(mtq.tests.fixture.test_func, 1, 2, a=1, b=2)
        self.assertFalse(job.finished())
        job = q.pop('worker')
        self.assertTrue(job.finished())

    def test_call_str(self):
        job = mtq.Job(self.factory, {'execute':{'func_str':'fs', 'args':(), 'kwargs':{}}})
        self.assertEqual(job.call_str, 'fs()')

        job = mtq.Job(self.factory, {'execute':{'func_str':'fs', 'args':(1, 2), 'kwargs':{}}})
        self.assertEqual(job.call_str, 'fs(1, 2)')

        job = mtq.Job(self.factory, {'execute':{'func_str':'fs', 'args':(1, 2), 'kwargs':{'a':3}}})
        self.assertEqual(job.call_str, 'fs(1, 2, a=3)')

        job = mtq.Job(self.factory, {'execute':{'func_str':'fs', 'args':(), 'kwargs':{'a':3}}})
        self.assertEqual(job.call_str, 'fs(a=3)')



if __name__ == '__main__':
    unittest.main()
