#!/usr/bin/env python
# encoding: utf-8
import unittest

import test_collectors, test_job, test_parsers, test_runner

def test_suite():
  return unittest.TestSuite((
    test_collectors.suite(),
    test_job.suite(),
    test_parsers.suite(),
    test_runner.suite()
  ))

if __name__ == '__main__':
  unittest.main(defaultTest='test_suite')