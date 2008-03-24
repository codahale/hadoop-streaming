#!/usr/bin/env python
# encoding: utf-8
import sys, unittest
from helpers import test
from optparse import OptionError

from hadoop.runner import Runner

class MockJob(object):
  def __init__(self):
    super(MockJob, self).__init__()
    self.actions = []
  
  def get_actions(self):
    x = self.actions
    self.actions = []
    return x
  
  def start_map(self):
    self.actions.append('start_map')
  
  def start_reduce(self):
    self.actions.append('start_reduce')

class RunnerTests(unittest.TestCase):
  # XXX : test more than just --map and --reduce
  def setUp(self):
    self.mock_job = MockJob()
    self.runner = Runner(self.get_mock_job)
  
  def get_mock_job(self):
    return self.mock_job
  
  @test
  def runner_should_start_mapping_if_told_to(self):
    self.runner.main(arguments=['--map'])
    self.assertEqual(['start_map'], self.mock_job.get_actions())
  
  @test
  def runner_should_start_reducing_if_told_to(self):
    self.runner.main(arguments=['--reduce'])
    self.assertEqual(['start_reduce'], self.mock_job.get_actions())
  

def suite():
  return unittest.makeSuite(RunnerTests)

if __name__ == '__main__':
  unittest.main()
