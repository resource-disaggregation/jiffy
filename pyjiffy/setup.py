#!/usr/bin/env python

from setuptools import setup

setup(name='pyjiffy',
      version='0.1.0',
      description='Python Client for jiffy',
      author='Anurag Khandelwal',
      author_email='anuragk@berkley.edu',
      url='https://www.github.com/ucbrise/jiffy',
      package_dir={'jiffy': 'jiffy'},
      packages=['jiffy', 'jiffy.storage', 'jiffy.directory', 'jiffy.lease', 'jiffy.notification', 'jiffy.subscription'],
      setup_requires=['pytest-runner>=2.0,<4.0', 'thrift>=0.11.0'],
      tests_require=['pytest-cov', 'pytest>2.0,<4.0', 'thrift>=0.11.0'],
      install_requires=['thrift>=0.11.0'])
