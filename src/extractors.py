"""
Extractor classes for the ELT pipeline.
"""
import os
from sys import exit
import time
#import pandas as pd
from pandas import DataFrame
from typing import Callable, List
from dataclasses import dataclass
from dotenv import load_dotenv
from ._baseclasses import BaseExtract
from .containers import Data
from src.etlkit import logging

load_dotenv()


#================================================================================#
@dataclass
class ExtractorJob:
	"""
	Container class for an extract job.
	"""
	query: str
	name: str
	extractor: BaseExtract

	def __init__(self, extractor: BaseExtract, name: str, query: str) -> None:
		self.query = query
		self.name = name
		self.extractor = extractor

		tests = [
			isinstance(query, str),
			isinstance(name, str),
			isinstance(extractor, BaseExtract)
		]
		if not all(tests):
			raise TypeError("ExtractorJob requires a query string, a name, and an extractor object.")
#================================================================================#

class SalesforceExtractor(BaseExtract):

	"""
	Class containing the extractor methods for simple / bulk Salesforce queries.
	"""

	#client: Salesforce | SalesforceBulk
	query_runner: Callable[[str], DataFrame]

	#______________________________________________________________________________#
	def __init__(self, bulk: bool = False):
		sf_username = os.getenv('SALESFORCE_USERNAME')
		sf_password = os.getenv('SALESFORCE_PASSWORD')
		sf_sectoken = os.getenv('SALESFORCE_SECTOKEN')

		if bulk:
			from salesforce_bulk import SalesforceBulk
			self.client = SalesforceBulk(username=sf_username,
																				password=sf_password,
																				security_token=sf_sectoken)
			self.query_runner = self._bulk_query
		else:
			from simple_salesforce import Salesforce
			self.client = Salesforce(username=sf_username,
																		password=sf_password,
																		security_token=sf_sectoken)
			self.query_runner = self._simple_query


	#______________________________________________________________________________#
	def _simple_query(self, query: str = '') -> DataFrame:
		"""
		Simple query using simple_salesforce.
		"""
		from simple_salesforce.exceptions import SalesforceMalformedRequest
		try:
			data = self.client.query_all(query) # type: ignore
		except SalesforceMalformedRequest:
			logging.error("Malformed query.")
			print('Query: ', query)
			exit()

		df = DataFrame(data['records'])
		df.drop(columns=['attributes'], inplace=True)
		return df


	#______________________________________________________________________________#
	def _bulk_query(self, query: str = '') -> DataFrame:
		"""
		Bulk query using salesforce_bulk.
		"""
		import unicodecsv

		sfbulk = self.client

		# Extract the object from the query
		obj = query.split('FROM ')[1].split('\n')[0]

		job = sfbulk.create_query_job(obj, contentType='CSV') # type: ignore
		batch = sfbulk.query(job, query) # type: ignore
		sfbulk.close_job(job) # type: ignore

		# Wait for the batch to complete
		logging.info("Waiting for batch to finish...")
		while not sfbulk.is_batch_done(batch): # type: ignore
			# Keep going until it's done, while displaying '.' every 5 seconds
			time.sleep(1)
			print('.', end='',flush=True)
		print()

		# Get the results
		data = []
		logging.info("Batch finished. Getting results...")
		for result in sfbulk.get_all_results_for_query_batch(batch): # type: ignore
			reader = unicodecsv.DictReader(result, encoding='utf-8')
			for row in reader:
				data.append(row)
		df = DataFrame(data)
		return df
#================================================================================#





#================================================================================#
class BigQueryExtractor(BaseExtract):

	"""
	Class containing the extractor methods for BigQuery queries.
	"""
	from google.cloud import bigquery
	project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')

	#______________________________________________________________________________#
	def __init__(self):
		self.client = self.bigquery.Client(self.project_id)
		self.query_runner = self._query

	#______________________________________________________________________________#
	def _query(self, query: str = '') -> DataFrame | Exception:
		"""
		Query BigQuery.
		"""
		import sys
		try:
			table = query.split('FROM ')[1].split('\n')[0]
			logging.info(f"Querying BigQuery table {table}")
			df = self.client.query(query).to_dataframe()
		except Exception as e:
			logging.error(e.__class__.__name__)
			print('\n\nQuery:\n', query)
			sys.exit()
		return df
#================================================================================#




#================================================================================#
class MultiExtractor:

	"""
	Class containing the extractor methods for multiple extract jobs.

	### Parameters:
	- extractor_jobs: a list of ExtractorJob objects. These are created
		using the create_job method.
	- async_extract: if True, the extractor will run the jobs asynchronously,
		which can be faster if there are a lot of jobs. Default is False.

	### Methods:
	* create_job: creates an ExtractorJob object and adds it to the
		`extractor_jobs` list.

	## Example usage:
	>>> # First, create an instance of the class
	>>> extractor = MultiExtractor()
	>>>
	>>> # Then, create the 'extract' jobs. Each job requires:
	>>> # - an extractor object (e.g. SalesforceExtractor())
	>>> # - a name for the job (e.g. 'salesforce')
	>>> # - a query string (e.g. 'SELECT Id FROM Account')
	>>> # Repeat this step for each job.
	>>> extractor.create_job(extractor=SalesforceExtractor(bulk=True),
		name='salesforce',
		query='SELECT Id FROM Account')
	>>> extractor.create_job(extractor=BigQueryExtractor(),
		name='bigquery',
		query='SELECT * FROM `my-project.my_dataset.my_table`')
	>>>
	>>> # Finally, run the extractor
	>>> data = extractor.run()
	"""
	extractor_jobs: List[ExtractorJob]

	#______________________________________________________________________________#
	def __init__(self, async_extract: bool = False):
		self.extractor_jobs: List[ExtractorJob] = []
		if async_extract:
			self.run = self._run_async

	#______________________________________________________________________________#
	def create_job(self, extractor: BaseExtract, name: str, query: str) -> None | Exception:
		"""
		Create an ExtractorJob object and add it to the list of jobs to complete.
		### Parameters:
		- extractor: the extractor object to use (e.g. `SalesforceExtractor()`, `BigQueryExtractor()`)
		- name: the name of the job.
		- query: the query string to run.

		### Example usage:
		>>> extractor = MultiExtractor()
		>>> extractor.create_job(extractor=SalesforceExtractor(bulk=True),
			name='salesforce',
			query='SELECT Id FROM Account')
		"""

		# Verify inputs:
		errors: List[Exception] = []
		if not isinstance(extractor, BaseExtract):
			errors.append(TypeError("extractor must be an instance of SalesforceExtractor or BigQueryExtractor."))
		if not isinstance(name, str):
			errors.append(TypeError("name must be a string."))
		if not isinstance(query, str):
			errors.append(TypeError("query must be an SQL query string."))
		if errors:
			for error in errors:
				logging.error(error)
			print(__doc__)
			raise TypeError("Invalid inputs.")

		job = ExtractorJob(extractor=extractor, name=name, query=query)
		self.extractor_jobs.append(job)


	#______________________________________________________________________________#
	def run(self) -> Data:
		"""
		Run the extractor jobs.
		### Returns:
		- data: a `Data` object containing the attribute `dataframes`, which is a `dict`
		containing the extracted dataframes.
		"""
		data = Data(dataframes={})
		for job in self.extractor_jobs:
			extractor = job.extractor
			name      = job.name
			query     = job.query
			df = extractor.query_runner(query)
			data.dataframes['df_'+name] = df
			logging.info(f"Extracted data from {name}")
		return data

	#______________________________________________________________________________#
	async def _extract_async(self) -> Data:
		"""
		Async function to run the jobs asynchronously.
		"""
		from asyncio import create_task as asyncio_create_task

		# Convert a function to an async function
		async def _asyncify(func: Callable, *args, **kwargs):
			return func(*args, **kwargs)

		# Build a list of async tasks
		data = Data(dataframes={})
		tasks = []
		for job in self.extractor_jobs:
			extractor = job.extractor
			name      = job.name
			query     = job.query
			task = asyncio_create_task(_asyncify(extractor.query_runner, query))
			tasks.append(task)

		# Build the job list
		for task, name in zip(tasks, [job.name for job in self.extractor_jobs]):
			df = await task
			data.dataframes['df_'+name] = df
			logging.info(f"Extracted data from {name}")
		return data

	def _run_async(self) -> Data:
		from asyncio import run as asyncio_run
		return asyncio_run(self._extract_async())
#================================================================================#
