#!/usr/bin/env python
# encoding: utf-8
import unittest

import test_parsers, test_collectors

def test_suite():
  return unittest.TestSuite((
    test_collectors.suite(),
    test_parsers.suite()
  ))

if __name__ == '__main__':
  unittest.main(defaultTest='test_suite')