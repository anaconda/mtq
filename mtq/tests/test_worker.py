'''
Created on Aug 5, 2013

@author: sean
'''

from mtq.tests.fixture import MTQTestCase
import mtq
import unittest
import mock

TEST_JOB_DATA = {'execute':{'func_str':'mtq.tests.fixture.test_func_fail',
                            'args':(1, 2), 'kwargs':{}},
                            '_id':'abc',
                            'qname':'qname',
                            'tags':[],
                        }

class TestWorker(MTQTestCase):

    def test_create_worker(self):
        worker = self.factory.new_worker(['q1', 'q2'], ['t1', 't2'])

        self.assertEqual(worker.queues, ['q1', 'q2'])
        self.assertEqual(worker.tags, ['t1', 't2'])

    def test_register_worker(self):
        worker = self.factory.new_worker(['q1', 'q2'], ['t1', 't2'])

        with worker.register():
            workers = list(self.factory.workers)
            self.assertEqual(len(workers), 1)
            workerpxy = workers[0]

            workerpxy.name

            self.assertEqual(workerpxy.qnames, ['q1', 'q2'])
            self.assertEqual(workerpxy.tags, ['t1', 't2'])
            self.assertEqual(workerpxy.num_processed, 0)
            self.assertFalse(workerpxy.finished())

        self.assertTrue(workerpxy.finished())

        workers = list(self.factory.workers)
        self.assertEqual(len(workers), 0)

    @mock.patch('mtq.worker.handle_signals')
    @mock.patch('mtq.tests.fixture.test_func')
    def test_sync_process(self, test_func, handle_signals):
        job = mtq.Job(self.factory, {'execute':{'func_str':'mtq.tests.fixture.test_func',
                                                'args':(1, 2), 'kwargs':{}},
                                     '_id':'abc',
                                     'qname':'qname',
                                     })

        worker = self.factory.new_worker(['q1', 'q2'], ['t1', 't2'])

        worker._process_job(job)
        handle_signals.assert_called_once_with()
        test_func.assert_called_once_with(1, 2)

        pre = mock.Mock('pre')
        post = mock.Mock('post')
        worker.set_pre(pre)
        worker.set_post(post)

        worker._process_job(job)
        pre.assert_called_once_with(job)
        post.assert_called_once_with(job)

    @mock.patch('mtq.worker.handle_signals')
    @mock.patch('mtq.tests.fixture.test_func')
    def test_sync_process_failure(self, test_func, handle_signals):
        job = mtq.Job(self.factory, {'execute':{'func_str':'mtq.tests.fixture.test_func_fail',
                                                'args':(1, 2), 'kwargs':{}},
                                     '_id':'abc',
                                     'qname':'qname',
                                     })

        worker = self.factory.new_worker(['q1', 'q2'], ['t1', 't2'])
        pre = mock.Mock('pre')
        post = mock.Mock('post')
        worker.set_pre(pre)
        worker.set_post(post)

        with self.assertRaises(Exception):
            worker._process_job(job)

        pre.assert_called_once_with(job)
        post.assert_called_once_with(job)

        exc_handler = mock.Mock('exc_handler')
        worker.push_exception_handler(exc_handler)

        with self.assertRaises(Exception):
            worker._process_job(job)

        self.assertEqual(exc_handler.call_count, 1)

    def test_process(self):
        job = mtq.Job(self.factory, {'execute':{'func_str':'mtq.tests.fixture.test_func',
                                                'args':(1, 2), 'kwargs':{}},
                                     '_id':'abc',
                                     'qname':'qname',
                                     'tags':[],
                                     })

        worker = self.factory.new_worker(['q1', 'q2'], ['t1', 't2'])

        failed = worker.process_job(job)
        self.assertFalse(failed)

    def test_process_error(self):
        job = mtq.Job(self.factory, {'execute':{'func_str':'mtq.tests.fixture.test_func_fail',
                                                'args':(1, 2), 'kwargs':{}},
                                     '_id':'abc',
                                     'qname':'qname',
                                     'tags':[],
                                     })

        worker = self.factory.new_worker(['q1', 'q2'], ['t1', 't2'], silence=True)

        failed = worker.process_job(job)
        self.assertTrue(failed)


    def test_tags(self):

        worker = self.factory.new_worker(['q1', 'q2'], ['linux-64', 'hostname:host1'], silence=True)
        q = self.factory.queue('q1')
        self.assertIsNone(worker.pop_item())

        q.enqueue_call('test')
        self.assertIsNotNone(worker.pop_item())

        q.enqueue_call('test', tags=['linux-64'])
        item = worker.pop_item()
        self.assertIsNotNone(item)
        self.assertEqual(item.tags, ['linux-64'])

        q.enqueue_call('test', tags=['hostname:host1'])
        item = worker.pop_item()
        self.assertIsNotNone(item)
        self.assertEqual(item.tags, ['hostname:host1'])

        q.enqueue_call('test', tags=['linux-64', 'hostname:host1'])
        item = worker.pop_item()
        self.assertIsNotNone(item)
        self.assertEqual(set(item.tags), set(['linux-64', 'hostname:host1']))

        q.enqueue_call('test', tags=['linux-64', 'hostname:host2'])
        self.assertIsNone(worker.pop_item())

        q.enqueue_call('test', tags=['linux-65'])
        self.assertIsNone(worker.pop_item())

        q.enqueue_call('test', tags=[])
        self.assertIsNotNone(worker.pop_item())

        self.assertIsNone(worker.pop_item())

    def test_unexpected_error_fail_fast(self):

        worker = self.factory.new_worker(['q1'], ['linux-64'], silence=True)
        worker.pop_item = mock.Mock()
        worker.pop_item.side_effect = KeyError("This is expected")

        with self.assertRaises(KeyError):
            worker.start_main_loop(fail_fast=True)

    def test_unexpected_error(self):

        worker = self.factory.new_worker(['q1'], ['linux-64'], silence=True)
        worker.pop_item = mock.Mock()
        worker.pop_item.return_value = None

        def side_effect(*_, **__):
            worker.pop_item.side_effect = None
            worker.pop_item.return_value = None
            raise KeyError("This is expected")

        worker.pop_item.side_effect = side_effect

        worker.start_main_loop(batch=True, fail_fast=False)




if __name__ == '__main__':
    unittest.main()
