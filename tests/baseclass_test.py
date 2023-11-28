import dotenv
import os
import pandas as pd
import inspect
from typing import Callable
from ..src._baseclasses import Config, Data
from ..src._baseclasses import BaseExtract, BaseLoad

dotenv.load_dotenv()
#================================================================================#




#==============================================================================#
#_____ Helper functions _______________________________________________________#
def get_return_type(function: Callable) -> str:
	"""
	Get the return type of a function.
	"""
	return inspect.getfullargspec(function).annotations['return']

#______________________________________________________________________________#
def get_input_types(function: Callable) -> dict:
	"""
	Get the input argument types of a function.
	"""
	return inspect.getfullargspec(function).annotations

#==============================================================================#



#______________________________________________________________________________#
def test_Config():
	config = Config()
	assert isinstance(config.lookbacktime, int)
	assert isinstance(config.min_date, str)
	assert isinstance(config.update_mode, bool)
	assert isinstance(config.table_name, str)
	assert isinstance(config.dataset_name, str)

	if os.getenv('LOCATION') == 'cloud':
		assert config.update_mode is True


#______________________________________________________________________________#
def test_Data():
	assert isinstance(Data(), Data)


#==============================================================================#
# Define dummy classes for testing
class DummyExtractor(BaseExtract):
	query = 'SELECT * FROM table'
	def query_runner(self, query: str = query) -> pd.DataFrame:
		return pd.DataFrame()


#______________________________________________________________________________#
class DummyLoader(BaseLoad):
	def load(self, data: pd.DataFrame,
					table_name: str = 'table',
					dataset_name: str = 'dataset'):
		pass



#==============================================================================#
#____ Test the base classes ___________________________________________________#
class TestBaseClasses:

	def test_BaseExtract(self):
		extractor = DummyExtractor()
		assert hasattr(extractor, 'query')
		assert hasattr(extractor, 'query_runner')
		assert isinstance(extractor.query, str)
		assert isinstance(extractor.query_runner(), pd.DataFrame)


	def test_BaseLoad(self):
		loader = DummyLoader()
		assert hasattr(loader, 'load')
		assert hasattr(loader, '__call__')
		assert loader.load(pd.DataFrame()) is None
#==============================================================================#



#==============================================================================#
#_____ Test that the classes in _extractors.py conform to the base class ______#
class TestExtractors:
	def test_generic_extractor(self):
		# Get list of classes in Extractors
		from ..src import extractors
		extractor_classes = [extractors.SalesforceExtractor, extractors.BigQueryExtractor]

		# Loop through the classes
		for extractor_class in extractor_classes:
			# Get the class
			extractor = extractor_class()
			# Check that it conforms to the base class
			assert isinstance(extractor, BaseExtract)
			assert hasattr(extractor, 'query_runner')
#==============================================================================#





#==============================================================================#
#_____ Test that the classes in _loaders.py conform to the base class _________#
# class TestLoaders:
# 	def test_generic_loader(self):
# 		# Get list of classes in loaders
# 		from ..src.loaders import BigQueryLoader # , GCPBucketLoader, GoogleSheetLoader
# 		loader_classes = [BigQueryLoader] # , GCPBucketLoader]#, GoogleSheetLoader]

# 		# Loop through the classes
# 		for loader_class in loader_classes:
# 			# Get the class
# 			loader = loader_class()
# 			# Check that it conforms to the base class
# 			assert isinstance(loader, BaseLoad)
# 			assert hasattr(loader, 'load')
# 			assert get_input_types(loader.load)['df'] == pd.DataFrame
#==============================================================================#
