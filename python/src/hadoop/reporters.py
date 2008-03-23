"""
  Debug reporters for Hadoop tasks.
"""

import sys

class Reporter(object):
  """A simple class which prints debug messages out to stderr."""
  def __init__(self, stream=sys.stderr, quiet=False):
    super(Reporter, self).__init__()
    self.stream = stream
    self.quiet = quiet
  
  def debug(self, message):
    if not self.quiet:
      self.stream.write('DEBUG: %s\n' % message)
  
