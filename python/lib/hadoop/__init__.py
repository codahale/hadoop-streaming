__author__ = "Coda Hale <coda.hale@gmail.com>"
__date__ = "2008-03-19"
__version__ = "1.0"
__credits__ = """
Copyright (c) 2008 Coda Hale

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

"""
  A set of classes which make writing map/reduce tasks for Hadoop easy.
"""

import sys

from hadoop.collectors import Collector
from hadoop.parsers import LineParser, KeyValueParser
from hadoop.runner import Runner

class Job(object):
  """
    The main Hadoop class. Your job classes should descend from this and
    implement map() and reduce().
  """
  def __init__(self):
    super(Job, self).__init__()
    self.map_parser, self.map_collector = LineParser, Collector
    self.reduce_parser, self.reduce_collector = KeyValueParser, Collector
  
  def start_map(self, parser_stream=sys.stdin, collector_stream=sys.stdout):
    """
      Starts the mapping process.
    """
    parser = self.map_parser(parser_stream)
    collector = self.map_collector(collector_stream)
    for data in parser:
      if isinstance(data, tuple):
        self.map(*(data + (collector,)))
      else:
        self.map(*(data, collector))
  
  def start_reduce(self, parser_stream=sys.stdin, collector_stream=sys.stdout):
    """
      Starts the reducing process.
    """
    parser = self.reduce_parser(parser_stream)
    collector = self.reduce_collector(collector_stream)
    self.reduce(parser, collector=collector)
  
  def map(self, line, collector):
    """
      Given a set of input values, generates a set of intermediate values and
      passes them to the reducer.
      
      This method *must* accept a named argument, collector, which is used to
      pass intermediate values to the reducers.
      
      The rest of the method signature depends on the output of the parser.
    """
    raise NotImplementedError('map() is not implemented in this class')
  
  def reduce(self, iterator, collector):
    """
      Given a set of keys and intermediate values, reduces and collects the
      final values.
      
      This method *must* accept a named argument, collector, which is used to
      collect final values.
    """
    raise NotImplementedError('reduce() is not implemented in this class')
  
  @classmethod
  def main(cls, arguments=sys.argv, runner=Runner):
    """
      Maps, reduces, or displays help based on command-line arguments.
    """
    runner(cls).main(arguments)