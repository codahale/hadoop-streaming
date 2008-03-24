#!/usr/bin/env python 
from distutils.core import setup 

setup(
  name='hadoop', 
  version='1.0', 
  description='Hadoop Streaming Support Library', 
  author='Coda Hale', 
  author_email='coda.hale@gmail.com',
  package_dir={ '' : 'lib' },
  packages=['hadoop']
) 
