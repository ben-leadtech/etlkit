import os
import dotenv
from dataclasses import dataclass
from pandas import DataFrame, Timestamp, Timedelta
from src.etlkit import logging

#_____ GLOBALS _____#
dotenv.load_dotenv()
ENVIRONMENT = os.getenv('ENVIRONMENT')
LOCATION    = os.getenv('LOCATION')


#================================================================================#
#_____ Config dataclass _________________________________________________________#
@dataclass
class Config:

	"""
	Config dataclass, containing the configuration optionsfor the ETL pipeline.

	Atributes:
		update_mode: whether to run in update mode or not. \n
		table_name: the name of the table to load the data into.
		dataset_name: the name of the dataset to load the data into.
		lookbacktime: the number of days to look back when running in update mode.
		min_date: the minimum date to look back to when *not* running in update mode.

	Notes:
		- If we're running in the cloud, we're always in update mode.
		- If we're running in update mode, the min_date is set to be lookbacktime days
			ago.
	"""


	def __init__(self,
							update_mode: bool = False,
							table_name: str = '',
							dataset_name: str = f'{ENVIRONMENT}_published',
							lookbacktime: int = 5,
							min_date: str = '2023-01-01T00:00:00Z'):
		self.update_mode   = update_mode
		self.table_name    = table_name
		self.dataset_name  = dataset_name
		self.lookbacktime  = lookbacktime
		self.min_date      = min_date

		# If we're running in the cloud, we're always in update mode
		if LOCATION == 'cloud':
			self.update_mode = True
			logging.info("Running in the cloud, setting update_mode to True")

		# If we're in update mode, set the min date to be lookbacktime days ago
		if update_mode:
			self.min_date = (Timestamp.now() - Timedelta(days=self.lookbacktime)).\
				strftime('%Y-%m-%dT%H:%M:%SZ')
			logging.info(f"Running in update mode, setting min_date to {self.min_date}")

	#______________________________________________________________________________#
	def __str__(self):
		output = ''
		for attr in self.__dict__:
			if not attr.startswith('_'):
				output += f"{attr}: {self.__dict__[attr]}\n"
		return output
#================================================================================#


@dataclass(repr=True)
class Data:
	"""
	Dataclass to hold the dataframes, e.g.:
	>>> data = Data(df1=df1, df2=df2)
	"""

	dataframes: dict

	#______________________________________________________________________________#
	def __init__(self, **kwargs):
		for key, value in kwargs.items():
			setattr(self, key, value)

	#______________________________________________________________________________#
	def __str__(self):
		output = 'Data object:'
		for attr in self.__dict__:
			if not attr.startswith('_'):
				var = self.__dict__[attr]
				if isinstance(var, DataFrame):
					var = var.head(5)
				output += f"""
--- {attr}: [{self.__dict__[attr].__class__.__name__}]
{var}
"""
		return output
