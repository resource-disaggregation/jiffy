#!/usr/bin/env python

from setuptools import setup

setup(name='elasticmem',
      version='0.1.0',
      description='Python Client for elasticmem',
      author='Anurag Khandelwal',
      author_email='anuragk@berkley.edu',
      url='https://www.github.com/ucbrise/elasticmem',
      package_dir={'elasticmem': 'elasticmem'},
      packages=['elasticmem.kv', 'elasticmem.directory'],
      setup_requires=['pytest-runner>=2.0,<4.0', 'thrift>=0.11.0'],
      tests_require=['pytest-cov', 'pytest>2.0,<4.0', 'thrift>=0.11.0'],
      install_requires=['thrift>=0.11.0'])
