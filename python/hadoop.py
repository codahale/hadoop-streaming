"""
A small set of utility functions for playing nicely as Hadoop Streaming tasks.
"""

import sys

def collect(callback):
  """
    Collects incoming Hadoop key/value pairs and executes a callback function
    for each.
    
    >>> def map_word_count(key, value, line):
    ...    map(hadoop.emit, word_counts(line))
    ...
    >>> hadoop.collect(map_word_count)
  """
  for line in sys.stdin:
    line = line.rstrip('\n')
    if '\t' in line:
      key, value = line.split('\t', 1)
    else:
      key, value = None, line
    callback(key, value, line)

def emit(key, value):
  """
    Send an intermediate or final key/value pair to the next stage of the
    map/reduce process. `value' should be a string or be coercable as such.
    
    >>> hadoop.emit('dingo', 300)
  """
  print '%s\t%s' % (key, value)

