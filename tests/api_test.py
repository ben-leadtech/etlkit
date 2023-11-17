"""
Test the API access for the conversion exporter.

We need to test access to:
- Google BigQuery
- Salesforce
"""
from google.cloud import bigquery
from src.etlkit.extractors import SalesforceExtractor
import dotenv
import os

dotenv.load_dotenv()
LOCATION = os.getenv('LOCATION')

#______________________________________________________________________________#
def test_bigquery():
	client = bigquery.Client()
	assert isinstance(client, bigquery.Client)
	test   = client.query("SELECT 1").result()
	assert test.total_rows == 1
	print("Google BigQuery API access is working.")

#______________________________________________________________________________#
def test_salesforce_simple():
	client = SalesforceExtractor(bulk=False).client
	try:
		test   = client.query_all("SELECT Id FROM Account LIMIT 1") # type: ignore
		assert test['totalSize'] == 1
		print("Salesforce API access is working.")
	except Exception as e:
		print("Salesforce API access is not working.")
		breakpoint()
		raise e

#______________________________________________________________________________#
def test_salesforce_bulk():
	connection = True
	try:
		_ = SalesforceExtractor(bulk=False)
	except Exception:
		connection = False
	assert connection

	# Only do this next test if we're in the cloud, since it takes a long time
	if LOCATION != 'local':
		query_runner = SalesforceExtractor(bulk=True).query_runner
		query = """
			SELECT Id
			FROM Account
			LIMIT 1
		"""
		try:
			test   = query_runner(query)
			assert len(test) == 1
		except Exception:
			connection = False
		assert connection
#______________________________________________________________________________#
