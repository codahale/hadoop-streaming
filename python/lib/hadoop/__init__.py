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

import sys

from hadoop.collectors import Collector
from hadoop.parsers import LineParser, KeyValueParser
from hadoop.runner import Runner

class Job(object):
  def __init__(self):
    super(Job, self).__init__()
    self.map_parser, self.map_collector = LineParser, Collector
    self.reduce_parser, self.reduce_collector = KeyValueParser, Collector
  
  def start_map(self, parser_stream=sys.stdin, collector_stream=sys.stdout):
    parser = self.map_parser(parser_stream)
    collector = self.map_collector(collector_stream)
    for data in parser:
      self.map(data, collector=collector)
  
  def start_reduce(self, parser_stream=sys.stdin, collector_stream=sys.stdout):
    parser = self.reduce_parser(parser_stream)
    collector = self.reduce_collector(collector_stream)
    self.reduce(parser, collector=collector)
  
  @classmethod
  def main(cls, arguments=sys.argv, runner=Runner):
    runner(cls).main(arguments)




# 
# 
# 
# 
# 
# 
# 
# 
# """
# A set of classes and functions which make writing Hadoop Streaming tasks fun.
# 
# For example:
# 
#   import hadoop
#   
#   class WordCount(hadoop.Job):
#     def map(self, key, value, line):
#       words = line.split(' ')
#       for word in words:
#         self.emit(word, 1)
#     
#     def reduce(self, keys_values_and_lines):
#       word_counts = dict()
#       for word, count, line in keys_values_and_lines:
#         word_counts[word] = word_counts.get(word, 0) + int(count)
#       for word, count in word_counts.iteritems():
#         self.emit(word, count)
# 
# To run:
#   
#   cat data.txt | word_count.py --map | word_count.py --reduce
#   
#   ./bin/hadoop jar ./contrib/streaming/hadoop-0.16.0-streaming.jar \\
#     -input whatever -output whatever -mapper word_count.py --map -reducer word_counts.py --reduce
# 
# """
# 
# 
# 
# 
# from itertools import imap
# import optparse
# import sys
# 
# class Job(object):
#   """
#     The base class for all Hadoop map/reduce jobs.
#   """
#   def __init__(self, log_debug=False):
#     super(Job, self).__init__()
#     self.log_debug = log_debug
#   
#   @classmethod
#   def main(cls):
#     """
#       If the first argument on the command line is 'map', starts the mapping
#       process. If it's 'reduce', starts the reduction process. Otherwise, displays
#       usage information.
#     """
#     p = optparse.OptionParser(usage='Usage: %prog [--debug] <--map|--reduce>')
#     p.add_option('-m', '--map', action='store_true', dest='map', default=False,
#                                 help='map input values to intermediary key/value pairs')
#     p.add_option('-r', '--reduce', action='store_true', dest='reduce', default=False,
#                                    help='reduce intermediary key/value pairs to final results')
#     p.add_option('-d', '--debug', action='store_true', dest='debug', default=False,
#                                     help='print debugging information to STDERR')
#     options, arguments = p.parse_args()
#     task = cls(log_debug=options.debug)
#     if options.map and options.reduce:
#       p.error("Need to either be mapping OR reducing here.")
#     elif options.map:
#       task.start_map()
#     elif options.reduce:
#       task.start_reduce()
#     else:
#       p.error("Need to either be mapping or reducing here.")
#     
#   
#   def __parse_line(self, line):
#     """
#       Parses a tab-separated line into a key (the first value) and values (all
#       subsequent values).
#       
#       >>> __parse_line('one\\ttwo\\tthree')
#       ('one', ['two', 'three'])
#     """
#     values = line.strip().split('\t')
#     key = values.pop(0)
#     return key, values
#   
#   def start_map(self):
#     """
#       Parses each line in STDIN and processes it with map().
#     """
#     for key, values in imap(self.__parse_line, sys.stdin):
#       self.map(key, values)
#   
#   def start_reduce(self):
#     """
#       Parses each line in STDIN and hands it to reduce() as an iterator.
#     """
#     self.reduce(imap(self.__parse_line, sys.stdin))
#   
#   def emit(self, key, *values):
#     """
#       Emits a key/values pair on STDOUT.
#       
#       >>> emit('dingo', 1, 2, 3)
#       dingo    1    2    3
#     """
#     if hasattr(values, '__iter__'):
#       values = '\t'.join(map(str, values))
#     print '%s\t%s' % (key, values)
#   
#   def debug(self, message, *values):
#     """
#       Prints message and various values on STDERR if --verbose has been specified.
#     """
#     if self.log_debug:
#       sys.stderr.write('DEBUG: %s\t%s\n' % (message, '\t'.join(map(repr, values))))