"""Arrange credentials for the extractors / loaders"""

import sys
import json
from dataclasses import dataclass
from . import logging



#================================================================================#
# Dataclasses
@dataclass
class GoogleCreds:
	"""Dataclass for Google credentials"""
	from google.oauth2.service_account import Credentials
	project_id: str
	creds: Credentials

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
	from google.oauth2.service_account import Credentials
	creds_json = read_json(jsonfile)
	if 'google_creds' not in creds_json:
		sys.exit(f"Credentials file {jsonfile} does not contain google_creds.")

	# Read the credentials file
	creds_info = read_json(creds_json['google_creds']['creds_file'])

	# Create the creds object
	try:
		creds = Credentials.from_service_account_info(creds_info)
	except Exception as e:
		sys.exit(f"Error creating credentials object: {e}")

	project_id = creds_json['google_creds']['project_id']
	return GoogleCreds(project_id, creds)
#================================================================================#



#================================================================================#
def report_usage_sf() -> None:
	"""Report usage for Salesforce"""
	logging.error("json file should contain the following:")
	logging.error("{")
	logging.error("    \"username\": \"[username]\",")
	logging.error("    \"password\": \"[password]\",")
	logging.error("    \"security_token\": \"[security_token]\"")
	logging.error("}")

#______________________________________________________________________________#
def get_salesforce_creds(jsonfile: str = '') -> SalesforceCreds:
	"""Get Salesforce client"""
	if jsonfile == '':
		logging.error("get_salesforce_creds: No credentials JSON file provided.")
		logging.error("set `salesforce_creds_json = `[path to JSON file]")
		report_usage_sf()
		raise ValueError("No credentials JSON file provided.")

	# Read the credentials file
	creds = read_json(jsonfile)

	# Validate the credentials
	required_keys = ['username', 'password', 'security_token']
	test_pass = True
	for key in required_keys:
		test_pass = test_pass and (key in creds)
	if not test_pass:
		logging.error("get_salesforce_creds: Credentials JSON file does not contain all required keys.")
		report_usage_sf()
		raise ValueError("Credentials JSON file does not contain all required keys.")

	# Create the creds object
	username = creds['username']
	password = creds['password']
	security_token = creds['security_token']
	return SalesforceCreds(username, password, security_token)
#================================================================================#
