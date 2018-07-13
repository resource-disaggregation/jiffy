#!/usr/bin/env python

from setuptools import setup

setup(name='pymmux',
      version='0.1.0',
      description='Python Client for mmux',
      author='Anurag Khandelwal',
      author_email='anuragk@berkley.edu',
      url='https://www.github.com/ucbrise/mmux',
      package_dir={'mmux': 'mmux'},
      packages=['mmux', 'mmux.kv', 'mmux.directory', 'mmux.lease', 'mmux.notification', 'mmux.subscription'],
      setup_requires=['pytest-runner>=2.0,<4.0', 'thrift>=0.11.0'],
      tests_require=['pytest-cov', 'pytest>2.0,<4.0', 'thrift>=0.11.0'],
      install_requires=['thrift>=0.11.0'])
