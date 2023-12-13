"""
Test the API access for the conversion exporter.

We need to test access to:
- Google BigQuery
- Salesforce
"""
from ..etlkit.extractors import SalesforceExtractor, BigQueryExtractor
import dotenv
import os

dotenv.load_dotenv()
LOCATION = os.getenv('LOCATION')

#______________________________________________________________________________#
def test_bigquery():
	from google.cloud.bigquery import Client
	creds_json = 'credentials.json'
	bq = BigQueryExtractor(creds_json=creds_json)
	client = bq.client
	assert isinstance(client, Client)
	datasets = list(client.list_datasets())
	assert len(datasets) > 0
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
