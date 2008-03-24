"""
  A basic runner for hadoop.Job classes.
"""

from optparse import OptionParser
import sys

class Runner(object):
  def __init__(self, job_cls, description=None, version=None):
    super(Runner, self).__init__()
    self.job_cls = job_cls
    self.description = description
    self.version = version
  
  def main(self, arguments=sys.argv):
    p = OptionParser(description=self.description, version=self.version)
    p.add_option('-m', '--map', action='store_true', dest='map', default=False,
                                help='map input values to intermediary key/value pairs')
    p.add_option('-r', '--reduce', action='store_true', dest='reduce', default=False,
                                   help='reduce intermediary key/value pairs to final results')
    options, arguments = p.parse_args(args=arguments)
    
    if (options.map and not options.reduce) or (options.reduce and not options.map):
      job = self.job_cls()
      if options.map:
        job.start_map()
      else:
        job.start_reduce()
    else:
      p.error("Need to either be mapping or reducing here.")
    
  
