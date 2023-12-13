"""
Extractor classes for the ELT pipeline.
"""
from sys import exit
import time
from pandas import DataFrame
from typing import Callable, List
from dataclasses import dataclass
from dotenv import load_dotenv
from ._baseclasses import BaseExtract
from .containers import Data
from . import logging

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


#================================================================================#
class SalesforceExtractor(BaseExtract):

	"""
	Class containing the extractor methods for simple / bulk Salesforce queries.
	"""

	#client: Salesforce | SalesforceBulk
	query_runner: Callable[[str], DataFrame]

	#______________________________________________________________________________#
	def __init__(self, bulk: bool = False, creds_json: str = ''):
		from .credentials import get_salesforce_creds

		# Get the credentials
		if creds_json == '':
			logging.error("SalesforceExtractor: No Salesforce credentials JSON file provided.")
			logging.error("SalesforceExtractor: set `salesforce_creds_json = `[path to JSON file]")
			self.query_runner = self._simple_query
			return None
			#raise ValueError("No Salesforce credentials JSON file provided.")
		creds = get_salesforce_creds(creds_json)

		# Create the client
		if bulk:
			from salesforce_bulk import SalesforceBulk
			self.client = SalesforceBulk(username=creds.username,
																		password=creds.password,
																		security_token=creds.security_token)
			self.query_runner = self._bulk_query
		else:
			from simple_salesforce import Salesforce
			self.client = Salesforce(username=creds.username,
																		password=creds.password,
																		security_token=creds.security_token)
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

	### Parameters:
	- creds_json: the path to the Google Credentials JSON file for BigQuery.
	"""
	from google.cloud.bigquery import Client as BigQueryClient

	#______________________________________________________________________________#
	def __init__(self, creds_json: str = ''):
		from google.oauth2.service_account import Credentials
		if creds_json == '':
			logging.error("BigQueryExtractor: No Google credentials JSON file provided.")
			logging.error("BigQueryExtractor: set `google_creds_json = `[path to JSON file]")
			self.query_runner = self._query
			return None
			#raise ValueError("No Google credentials JSON file provided.")

		creds = Credentials.from_service_account_file(creds_json)
		self.client = self.BigQueryClient(credentials=creds)
		self.query_runner = self._query

	#______________________________________________________________________________#
	def _query(self, query: str = '') -> DataFrame:
		"""
		Query BigQuery.
		"""

		buffer = query.split(self.client.project+'.')
		if len(buffer) > 1:
			table = buffer[1].split(' ')[0]
			logging.info(f"Querying BigQuery table {table}")

		try:
			df = self.client.query(query).to_dataframe()
		except Exception as e:
			logging.error("Error querying BQ:", e.__class__.__name__)
			print('\n\nQuery:\n', query)
			raise e
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
	run: Callable[[], Data]

	#______________________________________________________________________________#
	def __init__(self, async_extract: bool = False):
		self.extractor_jobs: List[ExtractorJob] = []
		if async_extract:
			self.run = self._run_async
		else:
			self.run = self._run

	#______________________________________________________________________________#
	def create_job(self, extractor: BaseExtract, name: str, query: str) -> None:
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
	def _run(self) -> Data:
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
