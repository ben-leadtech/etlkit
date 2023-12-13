import pytest
from typing import List
from unittest.mock import Mock, patch
import pandas as pd
from ..etlkit.containers import Data


def clear_cache(search_strings: List[str]):
	import sys
	for search_string in search_strings:
		for key in list(sys.modules.keys()):
			if search_string in key:
				del sys.modules[key]


#================================================================================#
class MockObjects:
	from salesforce_bulk import SalesforceBulk
	mock_sf_bulk_client = Mock(SalesforceBulk)
	mock_sf_bulk_client.query = Mock(return_value=pd.DataFrame())
	mock_sf_bulk_client.is_batch_done = Mock(return_value=True)
	mock_sf_bulk_client.get_all_results_for_query_batch = Mock(return_value={})

	from simple_salesforce import Salesforce
	mock_sf_client = Mock(Salesforce)
	mock_sf_client.query_all = Mock(return_value=pd.DataFrame())

	from google.cloud.bigquery import Client as BQClient
	mock_bq_client = Mock(BQClient)

	from google.oauth2.service_account import Credentials as GoogleCreds
	mock_google_creds = Mock(GoogleCreds)
	mock_google_creds.from_service_account_info = Mock(return_value=None)
	mock_google_creds.from_service_account_file = Mock(return_value=None)

	mock_service_account = Mock()
	mock_service_account.Credentials = Mock(GoogleCreds)
	mock_service_account.Credentials.from_service_account_info = Mock(return_value=None)
	mock_service_account.Credentials.from_service_account_file = Mock(return_value=None)

	#_ = clear_cache(['oogle','alesforce'])
	#del SalesforceBulk, Salesforce, BQClient, GoogleCreds
#================================================================================#

#______________________________________________________________________________#
def mock_get_salesforce_creds(*args, **kwargs):
	from ..etlkit.credentials import SalesforceCreds
	return SalesforceCreds('username','password','security_token')
#================================================================================#


#================================================================================#
def test_salesforce_extractors():
	with patch('simple_salesforce.Salesforce', new=MockObjects.mock_sf_client):
		with patch('salesforce_bulk.SalesforceBulk', new=MockObjects.mock_sf_bulk_client):
			with patch('etlkit.etlkit.credentials.get_salesforce_creds',
							new=mock_get_salesforce_creds):
				from ..etlkit.extractors import SalesforceExtractor
				sf_simp = SalesforceExtractor(bulk=False, creds_json='test.json')
				sf_bulk = SalesforceExtractor(bulk=True, creds_json='test.json')

	for sf in [sf_simp, sf_bulk]:
		assert hasattr(sf,'client')
		assert hasattr(sf,'query_runner')

	assert sf_simp.query_runner.__name__ == '_simple_query'
	assert sf_bulk.query_runner.__name__ == '_bulk_query'
#================================================================================#


#================================================================================#
def test_BigQueryExtractor():
	#_ = clear_cache(['oogle','Credentials'])
	with patch('google.oauth2.service_account.Credentials', new=MockObjects.mock_google_creds):
		with patch('google.cloud.bigquery.Client', new=MockObjects.mock_bq_client):
			with patch('etlkit.etlkit.credentials.get_bq_creds',
							new=MockObjects.mock_service_account):
				from ..etlkit.extractors import BigQueryExtractor
				bq = BigQueryExtractor(creds_json='test.json')
				assert hasattr(bq,'client')
				assert hasattr(bq,'query_runner')
				assert bq.client.__class__.__name__ == 'Client'
				assert bq.query_runner.__name__ == '_query'


	with pytest.raises(Exception):
		bq.query_runner('bad query')

	#with pytest.raises(ValueError):
	#	bq = BigQueryExtractor(creds_json='')

#================================================================================#


#================================================================================#
class TestMultiExtractor:
	# Test methods/attrs
	def test_mex_methods(self):
		from ..etlkit.extractors import MultiExtractor
		me = MultiExtractor()
		assert hasattr(me,'create_job')
		assert hasattr(me,'run')
		assert hasattr(me,'_extract_async')
		assert hasattr(me,'_run_async')
		assert hasattr(me,'extractor_jobs')

	#______________________________________________________________________________#
	# Test create_job
	def test_mex_create_job(self):
		with patch('simple_salesforce.Salesforce', new=MockObjects.mock_sf_client):
			with patch('etlkit.etlkit.credentials.get_salesforce_creds',
							new=mock_get_salesforce_creds):
				from ..etlkit.extractors import SalesforceExtractor, MultiExtractor
				me = MultiExtractor()
				me.create_job(query='query',name='name',
									extractor=SalesforceExtractor(creds_json='test.json'))

		assert me.extractor_jobs[0].query == 'query'
		assert me.extractor_jobs[0].name == 'name'
		assert me.extractor_jobs[0].extractor.__class__.__name__ == 'SalesforceExtractor'
		with pytest.raises(TypeError):
			me.create_job(query='query',name='name',extractor='extractor') # type: ignore

	#______________________________________________________________________________#
	# Test run
	def test_mex_run(self):
		from ..etlkit.extractors import SalesforceExtractor, MultiExtractor
		mock_sf = Mock(SalesforceExtractor)
		mock_sf.query_runner = Mock(return_value=pd.DataFrame())
		me = MultiExtractor()
		me.create_job(query='query',name='name',extractor=mock_sf)
		output = me.run()
		assert isinstance(output,Data)
		assert hasattr(output,'dataframes')
		assert output.dataframes['df_name'].empty
#================================================================================#
