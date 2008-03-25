"""
  Input parsers for Hadoop tasks.
"""
from itertools import imap

class LineParser(object):
  """
    A simple line-based parser. Each line is passed directly to the mapper or
    reducer as-is, with only the newline character stripped.
    
    >>> p = LineParser(sys.stdin)
    >>> lines = [line for line in p]
    ['blah', 'blee', 'blorg']
    
    Your map() or reduce() method should have the following profile:
      
      f(self, line, collector)
      
  """
  def __init__(self, stream):
    """
      Creates a new LineParser instance which will iterate over the provided
      iterable.
    """
    super(LineParser, self).__init__()
    self.stream = stream
  
  def __iter__(self):
    """
      Returns an iterator which returns each line coming in via the stream.
    """
    return imap(self.parse_line, self.stream)
  
  def parse_line(self, line):
    """
      Given a line, strips and returns it.
    """
    return line.strip()


class KeyValueParser(LineParser):
  """
    A key/value parser. Each key and value are separated by a tab, as per
    Hadoop Streaming.
    
    Your map() or reduce() method should have the following profile:
      
      f(self, key, value, collector)
  """
  def parse_line(self, line):
    """
      Given a line, strips it, and returns the key and value.
      
      >>> parser.parse_line('blah\\tdingo\\tyay\\n')
      ('blah', 'dingo\\tyay')
    """
    line = super(KeyValueParser, self).parse_line(line)
    key, value = line.split('\t', 1)
    return key, value    


class TSVParser(KeyValueParser):
  """
    A Tab-Separated Value parser. Each line is split by tabs and passed to the
    mapper or reducer as a (key, values) tuple.
    
    >>> p = TSVParser(sys.stdin)
    >>> lines = [line for line in p]
    [('key', ('1', '2', '3')), ('another', ('4', '5', '6'))]
    
    Your map() or reduce() method should have the following profile:
      
      f(self, key, values, collector)
  """
  def parse_line(self, line):
    """
      Parses a line of tab-separated text into tuples.
    """
    key, value = super(TSVParser, self).parse_line(line)
    return key, tuple(value.split('\t'))

