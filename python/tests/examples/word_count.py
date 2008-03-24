#!/usr/bin/env python
# encoding: utf-8

# only necessary because we want to use the non-installed version of the lib
import sys
sys.path.insert(0, 'lib')


from collections import defaultdict
from hadoop import Job

class WordCountJob(Job):
  """A sample word-count Hadoop job."""
  
  def map(self, line, collector):
    """Splits line into words, emits counts."""
    for word in line.split(' '):
      collector.collect(word.strip(), 1)
  
  def reduce(self, iterator, collector):
    """Reduces word counts, collects totals."""
    accumulator = defaultdict(int)
    for word, count in iterator:
      accumulator[word] += int(count)
    for word, total in accumulator.iteritems():
      collector.collect(word, total)

if __name__ == '__main__':
  WordCountJob.main()