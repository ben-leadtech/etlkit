"""
Base classes for ETL kit.
"""

import os
from abc import ABC, abstractmethod
from pandas import DataFrame
from dotenv import load_dotenv
from typing import Callable, Protocol
from src.etlkit import logging
from src.etlkit.containers import Config, Data

#_____ GLOBALS _____#
load_dotenv()
# Are we running locally or in the cloud?
LOCATION     = os.getenv('LOCATION')
PROJECT_ID   = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
ENVIRONMENT  = os.getenv('ENVIRONMENT')



#================================================================================#
#____ Generic ETL class, which ensures a run method is defined __________________#
# class BaseETL(ABC):

# 	"""
# 	Base class for the ETL pipeline.
# 	"""

# 	# Always have space for a config object, and allow it to be passed in
# 	config: Config = None
# 	def __init__(self, config: Config = None) -> None:
# 		if config is not None:
# 			self.config = config


# 	# Define the run method. This is defined in the child class.
# 	@abstractmethod
# 	def run(self, *args, **kwargs) -> Any:
# 		"""
# 		Run the ETL pipeline.
# 		"""
# 		pass

# 	# Define the call method, which calls the run method.
# 	def __call__(self, *args, **kwargs) -> Any:
# 		"""
# 		Call the run method.
# 		"""
# 		return self.run(*args, **kwargs)
#================================================================================#




#================================================================================#
#____ Generic ETL class, which ensures a run method is defined __________________#
# class BaseETLProtocol(Protocol):

# 	"""
# 	Base class for the ETL pipeline.
# 	"""

# 	# Always have space for a config object, and allow it to be passed in
# 	config: Config = None
# 	def __init__(self, config: Config = None) -> None:
# 		if config is not None:
# 			self.config = config


# 	# Define the run method. This is defined in the child class.
# 	def run(self, *args, **kwargs) -> Any:
# 		"""
# 		Run the ETL pipeline.
# 		"""
# 		pass


# 	# Define the call method, which calls the run method.
# 	def __call__(self, *args, **kwargs) -> Any:
# 		"""
# 		Call the run method.
# 		"""
# 		return self.run(*args, **kwargs)
#================================================================================#




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
#_____ Base class for the transform function ____________________________________#
# class BaseTransform(ABC):

# 	"""
# 	Base class for the transform function.

# 	Absract methods:
# 	- run: the function to transform the data. This is defined in the child class,
# 					but must take the input data as an argument.
# 					The type of the input data can be anything, but it must be the same
# 					type as the output of the extract function.
# 					The output of the transform function must be a dataframe.

# 	Notes:
# 	- This class also contains a __call__ method, which runs some checks to ensure
# 		the output conforms to expectations, before calling the 'run' function.
# 	"""

# 	@abstractmethod
# 	def run(self, input: Data) -> DataFrame:
# 		"""
# 		Generic runner that returns a dataframe of the transformed data.
# 		"""
# 		pass

# 	#______________________________________________________________________________#
# 	def __call__(self, input: any) -> DataFrame:
# 		"""
# 		Transform the data, run some checks to ensure it conforms to expectations,
# 		and return the transformed data.
# 		"""

# 		# TODO: add a check that the input is the same type as the output
# 		# of the extract function.



# 		# Run the transform function
# 		output = self.run(input)

# 		# Check the output is a dataframe
# 		if not isinstance(output, DataFrame):
# 			raise TypeError(f"Expected a dataframe, got {type(output)}")

# 		# Check that the output has a column called "Unique_ID"
# 		if 'Unique_ID' not in output.columns:
# 			raise KeyError("Expected a column called 'Unique_ID' in the output dataframe.")

# 		# Check that the values in the Unique_ID column are unique
# 		if not output['Unique_ID'].is_unique:
# 			raise ValueError("The values in the Unique_ID column are not unique.")

# 		logging.info("Transformed the data, all checks passed.")
# 		return output
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
