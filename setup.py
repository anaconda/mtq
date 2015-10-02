'''
@author: sean
'''

import sys

from setuptools import setup, find_packages

ctx = {}
try:
    with open('mtq/_version.py') as fd:
        exec(open('mtq/_version.py').read(), ctx)
    version = ctx.get('__version__', 'dev')
except IOError:
    version = 'dev'

setup(
    name='mtq',
    version=version,
    author='Continuum Analytics',
    author_email='sean.ross-ross@continuum.io',
    description='Mongo Task Queue',
    packages=find_packages(),

    install_requires=['pymongo>=2.8',
                      'python-dateutil>=2.1',
                      'pytz>=2013b',
                      ],
    entry_points={
          'console_scripts': [
              'mtq-worker = mtq.scripts.worker:main',
              'mtq-info = mtq.scripts.info:main',
              'mtq-tail = mtq.scripts.log:main',
              'mtq-scheduler = mtq.scripts.schedule:main',
              'mtq-ctrl = mtq.scripts.ctrl:main',
              ]
                 },

)

