"""
Base classes for ETL kit.
"""

import os
from abc import ABC, abstractmethod
from pandas import DataFrame
from dotenv import load_dotenv
from typing import Callable, Protocol
from src import logging
from src.containers import Config, Data

#_____ GLOBALS _____#
load_dotenv()
# Are we running locally or in the cloud?
LOCATION     = os.getenv('LOCATION')
PROJECT_ID   = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
ENVIRONMENT  = os.getenv('ENVIRONMENT')



#================================================================================#
#_____ Base class for the extract function ______________________________________#
class BaseExtract(ABC):

	"""
	Base class for the extract function.
	Parameters:
		query: the query string to run.
		query_runner: the function to run the query, e.g. SF.query(..)

	Notes:
	- This class also contains a __call__ method, which calls the query_runner
		function. This means it can be used as a function, e.g.: \n
		>>> extractor = Extractors.SalesforceExtractor(bulk=True) \n
		... which is required for use in the ETL pipeline base class.
	"""

	query: str = ''
	query_runner: Callable[[str], DataFrame | Exception]

	def __call__(self) -> DataFrame | Exception:
		"""
		Extract the data.
		"""
		return self.query_runner(self.query)
#================================================================================#




#================================================================================#
#_____ Base class for the load function _________________________________________#
class BaseLoad(ABC):

	"""
	Base class for the load function.

	Absract methods:
	- load: the function to load the data. This is defined in the child class,
					but must take the dataframe to upload and the table name as arguments.

	Notes:
	- This class also contains a __call__ method, which runs some checks to ensure
		the output conforms to expectations. It then calls the load function.
	"""

	@abstractmethod
	def load(self, df: DataFrame, table: str) -> None:
		"""
		Load the data.
		"""
		pass

	def __call__(self, df: DataFrame, config: Config) -> None:
		"""
		Load the data.
		"""

		# Check the table name is not empty
		if config.table_name == '':
			raise ValueError("Table name cannot be empty.")

		# Check the dataset name is not empty
		if config.dataset_name == '':
			raise ValueError("Dataset name cannot be empty.")

		# Check the data is a dataframe
		if not isinstance(df, DataFrame):
			raise TypeError(f"Expected a dataframe, got {type(df)}")

		# Check the data has a column called "Unique_ID"
		if 'Unique_ID' not in df.columns:
			raise KeyError("Expected a column called 'Unique_ID' in the output dataframe.")

		# If the dataframe is empty, don't do anything
		if df.empty:
			logging.info("Dataframe is empty, not loading anything.")
			return None

		self.update_mode  = config.update_mode
		self.table_name   = config.table_name
		self.dataset_name = config.dataset_name
		full_table_name   = f"{PROJECT_ID}.{self.dataset_name}.{self.table_name}"
		self.load(df, full_table_name)
#================================================================================#


#_____ Protocol class for the extract function ______________________________________#
class ExtractProtocol(Protocol):
	def run(self) -> Data:
		"""The `run` method must return a Data object."""
		...

#_____ Protocol class for the transform function ____________________________________#
class TransformProtocol(Protocol):
	def run(self, data: Data) -> DataFrame:
		"""The `run` method must accept a `Data` object and return a `DataFrame`."""
		...

#_____ Protocol class for the load function _________________________________________#
class LoadProtocol(Protocol):
	def run(self, df: DataFrame, config: Config) -> None:
		"""The `run` method must accept a `DataFrame` return `None`."""
		...
