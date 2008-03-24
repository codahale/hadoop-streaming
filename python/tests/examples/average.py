#!/usr/bin/env python
# encoding: utf-8

# only necessary because we want to use the non-installed version of the lib
import sys
sys.path.insert(0, 'lib')


from collections import defaultdict
from hadoop import Job
from hadoop.parsers import TSVParser

class AverageJob(Job):
  """Calculate the average value of a key/value TSV file."""
  def __init__(self):
    super(AverageJob, self).__init__()
    self.map_parser = TSVParser
  
  def map(self, key, values, collector):
    collector.collect(key, values[0])
  
  def reduce(self, iterator, collector):
    accumulator = defaultdict(list)
    for word, count in iterator:
      accumulator[word].append(int(count))
    for word, counts in accumulator.iteritems():
      collector.collect(word, sum(counts) / float(len(counts)))

if __name__ == '__main__':
  AverageJob.main()