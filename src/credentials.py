"""Arrange credentials for the extractors / loaders"""

import sys
import json
from dataclasses import dataclass



#================================================================================#
# Dataclasses
@dataclass
class GoogleCreds:
	"""Dataclass for Google credentials"""
	project_id: str
	creds_file: str

@dataclass
class SalesforceCreds:
	"""Dataclass for Salesforce credentials"""
	username: str
	password: str
	security_token: str
#================================================================================#



#================================================================================#
# Helper functions
def read_json(jsonfile: str) -> dict:
	"""Read a JSON file"""
	try:
		json_dict = json.load(open(jsonfile, 'r'))
	except FileNotFoundError:
		sys.exit(f"Credentials file {jsonfile} not found.")
	except json.decoder.JSONDecodeError:
		sys.exit(f"Credentials file {jsonfile} is not valid JSON.")
	except Exception as e:
		sys.exit(f"Unknown error reading credentials file {jsonfile}: {e}")
	return json_dict
#================================================================================#


#================================================================================#
def get_bq_creds(jsonfile: str = 'credentials.json') -> GoogleCreds:
	"""Get BigQuery client"""
	creds_json = read_json(jsonfile)
	if 'google_creds' not in creds_json:
		sys.exit(f"Credentials file {jsonfile} does not contain google_creds.")

	project_id = creds_json['google_creds']['project_id']
	creds_file = creds_json['google_creds']['creds_file']
	return GoogleCreds(project_id, creds_file)
#================================================================================#


#================================================================================#
def get_salesforce_creds(jsonfile: str = 'credentials.json') -> SalesforceCreds:
	"""Get Salesforce client"""
	creds_json = read_json(jsonfile)
	if 'salesforce_creds' not in creds_json:
		sys.exit(f"Credentials file {jsonfile} does not contain salesforce_creds.")

	username = creds_json['salesforce_creds']['username']
	password = creds_json['salesforce_creds']['password']
	security_token = creds_json['salesforce_creds']['security_token']
	return SalesforceCreds(username, password, security_token)
#================================================================================#
