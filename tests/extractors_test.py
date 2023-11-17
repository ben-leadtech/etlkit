import pytest
from unittest.mock import Mock
import pandas as pd
from src.etlkit.extractors import SalesforceExtractor, BigQueryExtractor
from src.etlkit.extractors import ExtractorJob, MultiExtractor
from src.etlkit.containers import Data

#================================================================================#
def test_ExtractorJob():
	ej = ExtractorJob(query='query',name='name',extractor=SalesforceExtractor())
	assert hasattr(ej,'query')
	assert hasattr(ej,'name')
	assert hasattr(ej,'extractor')

	with pytest.raises(TypeError):
		ej = ExtractorJob('query','name','extractor') # type: ignore

#================================================================================#
def test_extractors():
	extractors = [SalesforceExtractor(), BigQueryExtractor()]
	for ex in extractors:
		assert hasattr(ex,'client')
		assert hasattr(ex,'query_runner')

#================================================================================#
def test_salesforce_extractors():
	sf = SalesforceExtractor(bulk=True)
	assert hasattr(sf,'client')
	assert hasattr(sf,'query_runner')
	assert sf.client.__class__.__name__ == 'SalesforceBulk'
	assert sf.query_runner.__name__ == '_bulk_query'

	sf = SalesforceExtractor(bulk=False)
	assert hasattr(sf,'client')
	assert hasattr(sf,'query_runner')
	assert sf.client.__class__.__name__ == 'Salesforce'
	assert sf.query_runner.__name__ == '_simple_query'
#================================================================================#


#================================================================================#
def test_BigQueryExtractor():
	from google.cloud.exceptions import BadRequest
	bq = BigQueryExtractor()
	assert hasattr(bq,'client')
	assert hasattr(bq,'query_runner')
	assert bq.client.__class__.__name__ == 'Client'
	assert bq.query_runner.__name__ == '_query'

	with pytest.raises(BadRequest):
		bq.query_runner('bad query')
#================================================================================#


#================================================================================#
class TestMultiExtractor:

	# Test methods/attrs
	def test_mex_methods(self):
		me = MultiExtractor()
		assert hasattr(me,'create_job')
		assert hasattr(me,'run')
		assert hasattr(me,'_extract_async')
		assert hasattr(me,'_run_async')
		assert hasattr(me,'extractor_jobs')

	#______________________________________________________________________________#
	# Test create_job
	def test_mex_create_job(self):
		me = MultiExtractor()
		me.create_job(query='query',name='name',extractor=SalesforceExtractor())
		assert me.extractor_jobs[0].query == 'query'
		assert me.extractor_jobs[0].name == 'name'
		assert me.extractor_jobs[0].extractor.__class__.__name__ == 'SalesforceExtractor'
		with pytest.raises(TypeError):
			me.create_job(query='query',name='name',extractor='extractor') # type: ignore

	#______________________________________________________________________________#
	# Test run
	def test_mex_run(self):
		mock_sf = Mock(SalesforceExtractor)
		mock_sf.query_runner = Mock(return_value=pd.DataFrame())
		me = MultiExtractor()
		me.create_job(query='query',name='name',extractor=mock_sf)
		output = me.run()
		assert isinstance(output,Data)
		assert hasattr(output,'dataframes')
		assert output.dataframes['df_name'].empty
#================================================================================#