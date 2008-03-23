"""
  Input parsers for Hadoop tasks.
"""
from sys import stdin
from itertools import imap

class LineParser(object):
  """
    A simple line-based parser. Each line is passed directly to the mapper or
    reducer as-is, with only the newline character stripped.
    
    >>> p = LineParser(sys.stdin)
    >>> lines = [line for line in p]
    ['blah', 'blee', 'blorg']
  """
  def __init__(self, iterable=stdin):
    """
      Creates a new LineParser instance which will iterate over the provided
      iterable.
    """
    super(LineParser, self).__init__()
    self.iterable = iterable
  
  def __iter__(self):
    """
      Returns an iterator which returns each line coming in via the stream.
    """
    return imap(self.parse_line, self.iterable)
  
  def parse_line(self, line):
    """
      Given a line, strips and returns it.
    """
    return line.strip()

class TSVParser(LineParser):
  """
    A Tab-Separated Value parser. Each line is split by tabs and passed to the
    mapper or reducer as a (key, values) tuple.
    
    >>> p = TSVParser(sys.stdin)
    >>> lines = [line for line in p]
    [('key', ('1', '2', '3')), ('another', ('4', '5', '6'))]
  """
  def parse_line(self, line):
    """
      Parses a line of tab-separated text into tuples.
    """
    values = line.strip().split('\t')
    return values.pop(0), tuple(values)