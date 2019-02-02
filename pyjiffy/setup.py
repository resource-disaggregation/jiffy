#!/usr/bin/env python

from setuptools import setup
import os

jiffy_version = os.getenv('JIFFY_VERSION', 'unstable')
thrift_version = os.getenv('THRIFT_VERSION', '0.11.0')

setup(name='pyjiffy',
      version=jiffy_version,
      description='Python Client for jiffy',
      author='Anurag Khandelwal',
      author_email='anuragk@berkley.edu',
      url='https://www.github.com/ucbrise/jiffy',
      package_dir={'jiffy': 'jiffy'},
      packages=['jiffy', 'jiffy.storage', 'jiffy.directory', 'jiffy.lease', 'jiffy.notification', 'jiffy.subscription'],
      setup_requires=['pytest-runner>=2.0,<4.0', 'thrift>={}'.format(thrift_version)],
      tests_require=['pytest-cov', 'pytest>2.0,<4.0', 'thrift>={}'.format(thrift_version)],
      install_requires=['thrift>={}'.format(thrift_version)])
