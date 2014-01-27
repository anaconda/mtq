'''
@author: sean
'''

import sys

from setuptools import setup, find_packages

try:
    from mtq import __version__ as version
except ImportError:
    version = '?'
    
setup(
    name='mtq',
    version=version,
    author='Continuum Analytics',
    author_email='sean.ross-ross@continuum.io',
    description='Mongo Task Queue',
    packages=find_packages(),
    
    install_requires=['pymongo>=2.5',
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

