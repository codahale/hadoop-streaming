#!/usr/bin/env python
# encoding: utf-8
import unittest
from helpers import test

from hadoop.reporters import Reporter

class MockStdErr(object):
  def __init__(self):
    super(MockStdErr, self).__init__()
    self.lines = []
  
  def write(self, s):
    self.lines.append(s)
    

class ReporterTests(unittest.TestCase):
  def setUp(self):
    self.mock_stderr = MockStdErr()
    self.reporter = Reporter(stream=self.mock_stderr)
  
  @test
  def reporter_should_default_to_verbose(self):
    self.assertEqual(False, self.reporter.quiet)
  
  @test
  def reporter_should_output_strings(self):
    self.reporter.debug("This is a simple line.")
    self.assertEqual(["DEBUG: This is a simple line.\n"], self.mock_stderr.lines)
  
  @test
  def reporter_should_not_output_strings_if_quiet(self):
    self.reporter.quiet = True
    self.reporter.debug("DEBUG: This is a simple line.")
    self.assertEqual([], self.mock_stderr.lines)

def suite():
  return unittest.makeSuite(ReporterTests)

if __name__ == '__main__':
  unittest.main()