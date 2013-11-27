'''
@author: sean
'''

import sys

if 'develop' in sys.argv:
    # Don't import setuptools unless the user is actively trying to do
    # something that requires it.
    from setuptools import setup

else:
  from distutils.core import setup

setup(
    name='mtq',
    version="0.1.2",
    author='Continuum Analytics',
    author_email='sean.ross-ross@continuum.io',
    description='Mongo Task Queue',
    packages=['mtq'],
    
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

