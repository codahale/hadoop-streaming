#!/usr/bin/env python
# encoding: utf-8
import unittest
from helpers import test
from examples.word_count import WordCountJob

from hadoop import Job
from hadoop.parsers import LineParser, KeyValueParser
from hadoop.collectors import KeyValueCollector
import hadoop.runner


class MockStream(object):
  def __init__(self):
    super(MockStream, self).__init__()
    self.output = []
  
  def write(self, line):
    self.output.append(line)
  
  def read_output(self):
    x = self.output
    self.output = []
    return x


class MockRunner(object):
  def __init__(self, job_cls):
    MockRunner.job_cls = job_cls
    MockRunner.main_run = False
  
  def main(self, arguments):
    MockRunner.main_run = arguments


class JobTests(unittest.TestCase):
  def setUp(self):
    self.job = WordCountJob()
  
  @test
  def job_should_have_a_map_parser(self):
    self.assertEqual(LineParser, self.job.map_parser)
  
  @test
  def job_should_have_a_reduce_parser(self):
    self.assertEqual(KeyValueParser, self.job.reduce_parser)
  
  @test
  def job_should_have_a_map_collector(self):
    self.assertEqual(KeyValueCollector, self.job.map_collector)
  
  @test
  def job_should_have_a_reduce_collector(self):
    self.assertEqual(KeyValueCollector, self.job.reduce_collector)
  
  @test
  def job_should_map_parser_output_to_collector_input(self):
    s = MockStream()
    self.job.start_map(parser_stream=[], collector_stream=s)
    self.assertEqual([], s.read_output())
    self.job.start_map(parser_stream=['blah blee bloo'], collector_stream=s)
    self.assertEqual(['blah\t1\n', 'blee\t1\n', 'bloo\t1\n'], s.read_output())
  
  @test
  def job_should_reduce_parser_output_to_collector_input(self):
    s = MockStream()
    self.job.start_reduce(parser_stream=[], collector_stream=s)
    self.assertEqual([], s.read_output())
    self.job.start_reduce(parser_stream=['blah\t1', 'blah\t1', 'bloo\t1'], collector_stream=s)
    self.assertEqual(['blah\t2\n', 'bloo\t1\n'], sorted(s.read_output()))
  
  @test
  def job_should_be_runnable(self):
    WordCountJob.main(arguments=['--map'], runner=MockRunner)
    self.assertEqual(WordCountJob, MockRunner.job_cls)
    self.assertEqual(['--map'], MockRunner.main_run)
  

def suite():
  return unittest.makeSuite(JobTests)

if __name__ == '__main__':
  unittest.main()