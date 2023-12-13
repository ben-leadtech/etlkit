from ..etlkit.containers import Data, Config
from pandas import Timedelta, Timestamp, DataFrame


def test_Config():
	config = Config(update_mode=True)
	assert config.min_date == (Timestamp.now() - Timedelta(days=config.lookbacktime)).\
				strftime('%Y-%m-%dT%H:%M:%SZ')


def test_Data():
	data = Data(dataframes={'my_table':DataFrame()})
	assert hasattr(data,'dataframes')
	assert isinstance(data.dataframes,dict)
