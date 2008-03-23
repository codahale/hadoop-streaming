#!/usr/bin/env python
# encoding: utf-8
import unittest

import test_parsers, test_reporters

def test_suite():
  return unittest.TestSuite((
    test_parsers.suite(),
    test_reporters.suite()
  ))

if __name__ == '__main__':
  unittest.main(defaultTest='test_suite')