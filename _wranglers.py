"""
Data Wrangler (aka Transformer) classes
"""
import pandas as pd
from ._baseclasses import BaseTransform, Data, logging


#================================================================================#
# Data wrangling functions
class FieldHistoryWrangler(BaseTransform):
	"""
	Functions for the field history ELT pipeline
	"""

	#___________________________________________________________________________#
	def ohe_fh(self):
		"""
		One-hot encode the field history data, with the field names as the columns and
		the values as their timestamps
		"""
		logging.info("One-hot encoding the field history data...")
		df = self.data.df_fh.copy()

		# OHE the field history data
		df_ohe = pd.get_dummies(df['NewValue']).astype(int)
		df_tot = pd.concat([df, df_ohe], axis=1)

		# Replace the '1's with the timestamps, then determine the number of days
		# since the CreatedDate
		for col in df_ohe.columns:
			df_tot[col] = pd.to_datetime(df_tot[col] * df_tot['CreatedDate'])


		# Drop the columns we don't need
		cols_to_drop = ['NewValue', 'OldValue', 'CreatedDate', 'Id','']
		for col in cols_to_drop:
			if col in df_tot.columns:
				df_tot.drop(col, axis=1, inplace=True)


		# Replace any non alpha numeric / "_- "" characters in the column names
		df_tot.columns = df_tot.columns.\
											str.replace('[^a-zA-Z0-9_-]', ' ', regex=True).\
											str.replace('  ',' -')

		# Group by OpportunityId. We aggregate as 'min', as there should only be
		# one timestamp per column.
		df_tot.rename(columns={'OpportunityId':'Id'}, inplace=True)
		df_g = df_tot.groupby('Id').min().reset_index()

		# Store in self
		self.data.final = df_g


	#___________________________________________________________________________#
	def merge_opps(self):
		"""
		Merge with Opps, using the ID as the key, to get Partner_Lead_ID.
		"""
		df = self.data.df_opps.copy()
		df.rename(columns={'CreatedDate':'Created_Date',
											'Partner_Lead_ID__c': 'Partner_Lead_ID'}, inplace=True)
		df['Created_Date'] = pd.to_datetime(df['Created_Date'])
		df['Unique_ID'] = (df['Created_Date'].astype(str) + '_' + df['Id'].astype(str)).\
												str.replace(' ', '')

		logging.info("Merging the field history data with the opportunity data...")
		df = df.merge(self.data.final, on='Id', how='inner')

		# Replace timestamps in the OHE fields with the number of days since
		# 'Created_Date'.
		logging.info("Replacing timestamps with the number of days since 'Created_Date'...")
		df = self._ppd_time_delta(df)

		self.data.final = df


	#___________________________________________________________________________#
	def _ppd_time_delta(self, df:pd.DataFrame) -> pd.DataFrame:
		"""
		Replace timestamps in the OHE fields with the number of days since
		'Created_Date'.
		"""

		# Find the OHE columns. These are cols with timestamp dtypes but are
		# not called 'Created_Date'
		ohe_cols = []
		for col in df.columns:
			if 'datetime' in str(df[col].dtype) and col != 'Created_Date':
				df[col] = (pd.to_datetime(df[col]) - pd.to_datetime(df['Created_Date'])).\
					dt.days.astype('float')
				ohe_cols.append(col)

		# Drop those rows where all the OHE columns are null
		df.dropna(subset=ohe_cols, how='all', inplace=True)

		return df



	#___________________________________________________________________________#
	def run(self, data: Data) -> pd.DataFrame:
		"""
		Run the functions
		"""

		# First: is the data what we're expecting?
		if not isinstance(data, Data):
			raise TypeError(f"Expected a Data object, got {type(data)}")

		for attr in ['df_opps', 'df_fh']:
			attr_cur = getattr(data, attr, None)
			if attr_cur is None:
				raise ValueError(
					f"Expected a dataframe in the {attr} attribute.")
			if not isinstance(attr_cur, pd.DataFrame):
				raise TypeError(
					f"Expected a dataframe for {attr}, got {type(attr_cur)}")

		# Run the functions
		self.data = data
		self.ohe_fh()
		self.merge_opps()
		return self.data.final

#================================================================================#



