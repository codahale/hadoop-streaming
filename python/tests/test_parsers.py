#!/usr/bin/env python
# encoding: utf-8
import unittest
from helpers import test

from hadoop.parsers import LineParser, TSVParser

class ParserTests(unittest.TestCase):
  def setUp(self):
    self.fixture = [
      'one\ttwo\tthree\n',
      'four\tfive\tsix\n'
    ]
  
  @test
  def line_parser_should_parse_single_lines(self):
    p = LineParser(iterable=self.fixture)
    result = [l for l in p]
    self.assertEqual(2, len(result))
    self.assertEqual('one\ttwo\tthree', result[0])
    self.assertEqual('four\tfive\tsix', result[1])
  
  @test
  def tsv_parser_should_parse_tuples(self):
    p = TSVParser(iterable=self.fixture)
    result = [l for l in p]
    self.assertEqual(2, len(result))
    self.assertEqual(('one', ('two', 'three')), result[0])
    self.assertEqual(('four', ('five', 'six')), result[1])
  

def suite():
  return unittest.makeSuite(ParserTests)

if __name__ == '__main__':
  unittest.main()