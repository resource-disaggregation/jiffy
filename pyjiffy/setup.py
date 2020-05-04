#!/usr/bin/env python

from setuptools import setup
import os
import datetime

now = datetime.datetime.now()
jiffy_version = os.getenv('JIFFY_VERSION', str(now.year) + '.' + str(now.month))
thrift_version = os.getenv('THRIFT_PY_VERSION', '0.11.0')

setup(name='pyjiffy',
      version=jiffy_version,
      description='Python Client for jiffy',
      author='Anurag Khandelwal',
      author_email='anurag.khandelwal@yale.edu',
      url='https://www.github.com/ucbrise/jiffy',
      package_dir={'jiffy': 'jiffy'},
      packages=['jiffy', 'jiffy.storage', 'jiffy.directory', 'jiffy.lease'],
      setup_requires=['pytest-runner>=2.0,<4.0', 'thrift>={}'.format(thrift_version)],
      tests_require=['pytest-cov', 'pytest>2.0,<4.0', 'thrift>={}'.format(thrift_version)],
      install_requires=['thrift>={}'.format(thrift_version)])
