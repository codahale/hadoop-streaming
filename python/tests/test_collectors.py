#!/usr/bin/env python
# encoding: utf-8
import unittest
from helpers import test

from hadoop.collectors import Collector, TSVCollector

class MockStdOut(object):
  def __init__(self):
    super(MockStdOut, self).__init__()
    self.lines = []
  
  def write(self, s):
    self.lines.append(s)
  
  def read_lines(self):
    x = self.lines
    self.lines = []
    return x

class CollectorTests(unittest.TestCase):
  def setUp(self):
    self.stdout = MockStdOut()
  
  @test
  def collector_should_output_key_and_value_to_stdout(self):
    collector = Collector(stream=self.stdout)
    collector.collect('key', 'value')
    self.assertEqual(['key\tvalue\n'], self.stdout.read_lines())
  
  @test
  def tsv_collector_should_output_key_and_values(self):
    collector = TSVCollector(stream=self.stdout)
    collector.collect('key', 1, 2, 3, 4)
    self.assertEqual(['key\t1\t2\t3\t4\n'], self.stdout.read_lines())
    collector.collect('key', values=(1, 2, 3, 4))
    self.assertEqual(['key\t1\t2\t3\t4\n'], self.stdout.read_lines())


def suite():
  return unittest.makeSuite(CollectorTests)

if __name__ == '__main__':
  unittest.main()