"""
Tools for loading data into databases and/or cloud storage.
"""
import pandas as pd
from src.etlkit import logging
from ._baseclasses import BaseLoad
from src.ingestors.salesforce_exporter import export_to_bigquery, update_bigquery
from src.ingestors.salesforce_exporter import delete_table, create_table
from src.ingestors.salesforce_exporter import schema_from_dataframe
from src.ingestors.support.db import client as client_bq


#================================================================================#
class BigQueryLoader(BaseLoad):

	"""
	Class containing the loader methods for BigQuery. Inherits from BaseLoad,
	so the only method that needs to be implemented is load().
	"""

	#______________________________________________________________________________#
	def _write_from_scratch(self, df: pd.DataFrame, table: str = ''):

		"""
		Write the dataframe to a new table.
		"""

		logging.info("Getting the schema from the dataframe.")
		schema_agent = schema_from_dataframe(df,
														convert_integer=False, convert_floating=True)

		#_____ Delete any existing table _____#
		logging.info(f"Deleting table {table}.")
		try:
			delete_table(client=client_bq, table=table)
		except Exception:
			logging.info(f"Table {table} does not exist.")

		#_____ Create the new table _____#
		logging.info(f"Creating table {table}.")
		try:
			create_table(client=client_bq, table=table, schema=schema_agent)
		except Exception:
			breakpoint()

		#_____ Upload the data _____#
		logging.info(f"Uploading the dataframe to BQ as table {table}.")
		try:
			export_to_bigquery(df, schema=schema_agent,
													table=table, client=client_bq)
		except Exception:
			breakpoint()
		logging.info(f"Exported {table}")


	#______________________________________________________________________________#
	def _update_table(self, df: pd.DataFrame, table: str = ''):

		"""
		Update the data in the table.
		"""

		# There's a chance that there are columns missing from the dataframe that
		# are in the table. This is because these dispositions didn't show up in the
		# last LOOKBACKTIME days. We need to add these columns to the dataframe, and
		# fill them with NaNs.
		df = self._fill_in_missing_cols(df, table)


		logging.info(f"Updating table {table}.")
		update_bigquery(client=client_bq, table=table, df=df)


	#______________________________________________________________________________#
	def _fill_in_missing_cols(self, df: pd.DataFrame, table: str = '') -> pd.DataFrame:

		"""
		Fill in any columns that are in the table but not in the dataframe.
		"""
		from numpy import nan as np_nan

		# Get the schema of the table
		schema = client_bq.get_table(table).schema

		# Get the names of the columns in the table
		cols_table   = [field.name for field in schema]

		# Get the names of the columns in the dataframe
		cols_df = df.columns.tolist()

		# Get the columns that are in the table but not in the dataframe
		cols_missing = [col for col in cols_table if col not in cols_df]

		# Add these columns to the dataframe, with NaNs
		for col in cols_missing:
			df[col] = np_nan

		return df



	#______________________________________________________________________________#
	def load(self, df: pd.DataFrame, table: str = ''):
		"""
		Load the data.
		"""

		if self.update_mode:
			self._update_table(df, table)
		else:
			self._write_from_scratch(df, table)

#================================================================================#




#================================================================================#
class GCPBucketLoader(BaseLoad):

	"""
	Class containing the loader methods for writing `DataFrame` objects to GCP buckets.
	Inherits from BaseLoad, so the only method that needs to be implemented is load().
	"""


	def __init__(self, bucket_name: str = '', project_id: str | None = ''):
		from google.cloud import storage
		self.client = storage.Client(project=project_id)
		self.bucket = self.client.bucket(bucket_name)

	#______________________________________________________________________________#
	def load(self, df: pd.DataFrame, table: str = ''):
		"""
		Load the data.
		"""
		table = self.table_name # overide the table name with that from the config

		# Write the dataframe to a csv file
		logging.info("Writing the dataframe to a csv file.")
		df.to_csv('temp.csv', index=False)
		df.to_csv(f"gs://{self.bucket.name}/{table}.csv", index=False)
#================================================================================#



#================================================================================#
class GoogleSheetLoader(BaseLoad):

	"""
	Class containing the loader methods for writing `DataFrame` objects to Google Sheets.
	Inherits from BaseLoad, so the only method that needs to be implemented is load().
	"""
	import gspread
	gc: gspread.Client
	share_list: list
	worksheet: gspread.Worksheet
	spreadsheet: gspread.Spreadsheet


	def __init__(self, sheet_name: str = ''):
		import gspread
		import os
		import dotenv
		from oauth2client.service_account import ServiceAccountCredentials
		dotenv.load_dotenv()
		google_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
		credentials = ServiceAccountCredentials.from_json_keyfile_name(google_creds,
					['https://spreadsheets.google.com/feeds',
						'https://www.googleapis.com/auth/drive']) # type: ignore
		gc = gspread.authorize(credentials)

		#_________ Initialise the spreadsheet:
		# Check if the spreadsheet exists, and create it if not
		try:
			spreadsheet = gc.open(sheet_name)
			worksheet   = spreadsheet.get_worksheet(0)
			worksheet.clear()
			logging.info(f"Spreadsheet {sheet_name} exists. Clearing it.")
		except self.gspread.exceptions.SpreadsheetNotFound:
			spreadsheet = gc.create(sheet_name)
			worksheet   = spreadsheet.get_worksheet(0)
			logging.info(f"Spreadsheet {sheet_name} does not exist. Creating it.")

		self.gc = gc
		self.worksheet = worksheet
		self.spreadsheet = spreadsheet


	#______________________________________________________________________________#
	def share_with(self):
		"""
		Share the spreadsheet with the share list.
		"""
		if len(self.share_list) > 0:
			logging.info(f"Sharing spreadsheet {self.spreadsheet.title} with {self.share_list}")
			for email in self.share_list:
				self.spreadsheet.share(email, perm_type='user', role='reader', notify=False)
				self.spreadsheet.share(email, perm_type='user', role='writer', notify=False)



#______________________________________________________________________________#
	def load(self, df: pd.DataFrame, table: str = ''):
		"""
		Write the results to a Google Sheet
		"""
		from gspread_dataframe import get_as_dataframe, set_with_dataframe


		#_________ Chop the dataframe into chunks
		chunk_size = 3000
		df_chunks = []
		for i in range(0, len(df), chunk_size):
			i0 = i
			i1 = min([i + chunk_size, len(df)])
			df_chunks.append(df.iloc[i0:i1, :])


		#_________ Write the chunks to the Google Sheet
		logging.info(f"Writing {len(df)} rows to Google Sheet in chunks of {chunk_size} rows.")
		self.worksheet.insert_row(["Parameters:TimeZone=+0000"],1)
		for chunk in df_chunks:
			existing_data = get_as_dataframe(self.worksheet)

			# Drop any columns that are in the existing data but not in the chunk
			cols_to_drop = []
			for col in existing_data.columns:
				if col not in chunk.columns:
					cols_to_drop.append(col)
			existing_data.drop(columns=cols_to_drop, inplace=True)

			# Drop rows that are all null
			existing_data.dropna(how='all', inplace=True)

			if len(existing_data) > 0:
				chunk = pd.concat([existing_data, chunk], ignore_index=True, axis=0)
			set_with_dataframe(self.worksheet, chunk, include_index=False,
											include_column_header=True, row=2, resize=True)

		#_________ Share the spreadsheet
		self.share_with()

		logging.info(f"Upload complete. Static URL = {self.spreadsheet.url}")
#================================================================================#
