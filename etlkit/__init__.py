"""
Extract, Transform, Load Kit
"""

#______________________________________________________________________________#
#class Transformers:
#	from ._wranglers import FieldHistoryWrangler
import logging
logging.\
	basicConfig(level=logging.INFO,
#			format='%(asctime)s: %(levelname)s ==== %(filename)s:%(module)s:%(funcName)s: %(message)s',
			format='%(asctime)s: ETLkit - %(levelname)s ==== %(module)s: %(message)s',
			datefmt='%H:%M:%S')

