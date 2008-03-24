"""
  Output collectors for Hadoop tasks.
"""
class Collector(object):
  """
    A basic string/string collector for key/value pairs.
    
    >>> collector.collect('key', 'value')
    key   value
  """
  def __init__(self, stream):
    """
      Creates a new Collector instance which outputs data to the provided
      stream.
    """
    super(Collector, self).__init__()
    self.stream = stream
  
  def collect(self, key, value):
    """
      Passes a key/value pair along to the next stage of processing.
    """
    self.stream.write('%s\t%s\n' % (key, value))
  

class TSVCollector(Collector):
  """
    A collector which outputs multiple, tab-separated values.
    
    >>> collector.collect('key', 1, 2, 3, 4)
    key   1   2   3   4
    >>> collector.collect('key', values=[1, 2, 3])
    key   1   2   3
  """
  def collect(self, key, *values, **kwargs):
    """
      Passes a key and multiple values along to the next stage of processing.
    """
    formatted_values = '\t'.join(map(str, kwargs.get('values', values)))
    super(self.__class__, self).collect(key, formatted_values)


