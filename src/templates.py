import sys
import dotenv
import os
from typing import Any, List, Protocol, Type, Optional
from pandas import DataFrame
from src.etlkit import logging
from src.etlkit.containers import Data, Config

#_____ GLOBALS _____#
dotenv.load_dotenv()
ENVIRONMENT = os.getenv('ENVIRONMENT')
LOCATION    = os.getenv('LOCATION')


#================================================================================#
# Helper functions ______________________________________________________________#
#================================================================================#
def generic_checker(test: bool,
										msg: str,
										exception: Type[Exception]) -> None | Type[Exception]:
	"""
	Checks a generic condition, and throws an exception if the condition is not met.
	"""
	if test:
		return None

	logging.error(msg)
	return exception

#________________________________________________________________________________#
def generic_check_handler(tests: List[tuple[bool,str,Type[Exception]]] ) -> None:
	"""
	Checks a list of generic conditions, and throws an exception if any of the
	conditions are not met.
	"""
	exceptions = []
	for test in tests:
		exceptions.append(generic_checker(*test))

	# Did any errors get thrown?
	if any([x is not None for x in exceptions]):
		if LOCATION == 'local':
			logging.error("Errors were thrown, entering debug mode.")
			raise Exception(exceptions)
		else:
			logging.error("Errors were thrown, raising exception.")
			raise Exception(exceptions)


#________________________________________________________________________________#
def load_checker(df: DataFrame, config: Config) -> None:
	# Check the table name is not empty
	tests = [
		(config.table_name != '', "Table name cannot be empty.", ValueError),
		(config.dataset_name != '', "Dataset name cannot be empty.", ValueError),
		(isinstance(df, DataFrame), f"Expected a dataframe, got {type(df)}", TypeError),
		('Unique_ID' in df.columns, "No 'Unique_ID' col in the output.", KeyError),
		(df['Unique_ID'].is_unique, "The 'Unique_ID' col is not unique.", ValueError),
	]

	generic_check_handler(tests)

	# If the dataframe is empty, don't do anything
	if df.empty:
		logging.warning("Dataframe is empty, not loading anything.")
		return None

#________________________________________________________________________________#
def transform_checker(output: DataFrame) -> None:

	tests = [
		(isinstance(output, DataFrame), f"Expected a dataframe, got {type(output)}", TypeError),
		('Unique_ID' in output.columns, "No 'Unique_ID' col in the output.", KeyError),
		(output['Unique_ID'].is_unique, "The 'Unique_ID' col is not unique.", ValueError),
	]

	generic_check_handler(tests)
#================================================================================#




#================================================================================#
#_____________________ Templates for the ETL components _________________________#

#_____ Protocol class for the extract function __________________________________#
class ExtractTemplate(Protocol):

	"""
	Protocol class for the extract function.\n

	Must contain a `run` method that returns a `Data` object.

	Optionally, you can specify a `Config` object at instantiation. If you do, it will
	be available to the `run` method.\n

	Objects to do the extracting can be found in `src.etlkit.extractors`.

	## Example:
	>>> class MyExtractor(ExtractTemplate):
	>>>   # define the extract function
	>>>   def extract(self) -> Data:
	>>>     return BigQueryExtractor().run()
	>>>   # define the run function
	>>>   def run(self) -> Data:
	>>>     return self.extract()
	"""
	config: Config

	def __init__(self, config: Optional[Config] = None) -> None:
		"""
		The `config` object is optional. If specified at instantiation, it will be
		available to the `run` method.
		"""
		if config is not None:
			self.config = config

	def run(self) -> Data:
		"""The `run` method must return a Data object."""
		...



#_____ Protocol class for the transform function ________________________________#
class TransformTemplate(Protocol):

	"""
	Protocol class for the transform function.

	Must contain a `run` method that accepts a `Data` object and returns a
	`DataFrame`.

	This part of the pipeline will be highly specific to the job. Try to ensure that
	this class is built of small, single-responsibility functions that can be tested
	independently. The `run` method should be a simple wrapper that calls these
	functions in the correct order, and returns the final `DataFrame`.

	## Example:
	>>> class MyTransformer(TransformTemplate):
	>>>   # define the transform function
	>>>   def transform(self, data: Data) -> DataFrame:
	>>>     df1 = data.dataframes['my_table1']
	>>>     df2 = data.dataframes['my_table2']
	>>>     df = df1.merge(df2, on='Unique_ID')
	>>>     return df
	>>>
	>>>   # define the run function
	>>>   def run(self, data: Data) -> DataFrame:
	>>>     return self.transform(data)
	"""

	#______________________________________________________________________________#
	def throw_errors(self, errors: List[Exception]) -> None:
		"""Throw errors if there are any."""
		if len(errors) == 0:
			return None

		for error in errors:
			logging.error(error)
		logging.error("Exiting")
		sys.exit()

	#______________________________________________________________________________#
	def test_inputs(self, data: Data) -> None:
		"""Test that the inputs are valid."""
		dataframes = data.dataframes
		errors: List[Exception] = []

		if not isinstance(dataframes, dict):
			errors.append(TypeError(f"Expected a dict, got {type(dataframes)}"))
			self.throw_errors(errors)

		for key, value in dataframes.items():
			if not isinstance(value, DataFrame):
				errors.append(TypeError(f"Expected a DataFrame, got {type(value)}"))
			if value.empty:
				errors.append(ValueError(f"Dataframe {key} is empty."))

		# If there are errors, throw them
		self.throw_errors(errors)

	#______________________________________________________________________________#
	def run(self, data: Data) -> DataFrame:
		"""The `run` method must accept a `Data` object and return a `DataFrame`."""
		...



#_____ Protocol class for the load function _____________________________________#
class LoadTemplate(Protocol):

	"""
	Protocol class for the load function.

	Must contain a `run` method that accepts a `DataFrame` and a `Config` object,
	and returns `None`.

	Objects to do the loading can be found in `src.etlkit.loaders`.

	## Example:
	>>> class MyLoader(LoadTemplate):
	>>>   # define the load function
	>>>   def load(self, df: DataFrame, config: Config) -> None:
	>>>     BigQueryLoader()(df, config)
	>>>   # define the run function
	>>>   def run(self, df: DataFrame, config: Config) -> None:
	>>>     self.load(df, config)

	"""

	def run(self, df: DataFrame, config: Config) -> None:
		"""The `run` method must accept a `DataFrame` return `None`."""
		...
#================================================================================#




#================================================================================#
#_____________________ TemplatePipeline class ___________________________________#
class TemplatePipeline:

	"""
	The blueprint ETL pipeline. This class is a wrapper for the extract, transform,
	and load functions. It is designed to be subclassed, with the extract, transform,
	and load functions being implemented in the subclass. The templates for these
	subclasses are defined in `src.etlkit.templates`.

	## Inputs:
	* `Extractor` class (must inherit from `ExtractTemplate`)
	* `Transformer` class (must inherit from `TransformTemplate`)
	* `Loader` class (must inherit from `LoadTemplate`)
	* `Config` object (from `src.etlkit.containers`)

	Once the pipeline is instantiated, it can be run by calling the object. The `run`
	method will run the extractor, transformer, and loader in sequence, applying checks
	at each stage.

	## Example:
	>>> my_pipeline = TemplatePipeline(MyExtractor, MyTransformer, MyLoader, config)
	>>> my_pipeline()
	"""


	def __init__(self,
							extractor: Type[ExtractTemplate],
							transformer: Type[TransformTemplate],
							loader: Type[LoadTemplate],
							config: Config) -> None:
		"""
		Define the extract, transform, and load functions.
		"""
		super().__init__()
		self.extractor   = extractor
		self.transformer = transformer
		self.loader      = loader
		self.config      = config
	#______________________________________________________________________________#

	def extract(self) -> Data:
		"""Extract the data."""
		data = self.extractor(self.config).run()
		return data


	def transform(self, data: Any) -> DataFrame:
		"""Transform the data."""
		df = self.transformer().run(data)
		transform_checker(df)
		return df


	def load(self, df: DataFrame, config: Config) -> None:
		"""Load the data."""
		load_checker(df, config)
		self.loader().run(df, config)
		return None


	def run(self) -> None:
		if self.config is None:
			logging.error("Config is None, cannot run the pipeline.")
			logging.error("Exiting")
			sys.exit()

		data = self.extract()
		df   = self.transform(data)
		self.load(df, self.config)

	def __call__(self) -> None:
		self.run()

#================================================================================#





