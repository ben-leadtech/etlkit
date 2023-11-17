#
# Development
#

# variables, targets, dependencies, and recipes
TESTER = pytest
TESTER_ARGS = -ra --verbose --disable-warnings
LINTER = flake8
LINTER_ARGS = --verbose



test:
	echo "Do some tests or whatever"
	make test-ingestors
	make test-paidopt
	make test-dataviz
	make test-etlkit


lint:
	echo "Do some linting or whatever"
	make lint-all



#===============================================================================
#
# Runners


run-all:
	make run-salesforce-opportunities
	make run-salesforce-accounts
	make run-paid-ingestor
	make run-adthena-ingestor
	make run-MPE-converter
	make run-MPE2SF

run-salesforce-opportunities:
	pipenv run python src/ingestors/salesforce_opportunities.py

run-salesforce-accounts:
	pipenv run python src/ingestors/salesforce_accounts.py

run-salesforce-exporter-account:
	pipenv run python src/ingestors/salesforce_exporter_account.py

run-salesforce-exporter-opportunities:
	pipenv run python src/ingestors/salesforce_exporter_opportunities.py

run-salesforce-exporter-cx:
	pipenv run python src/ingestors/salesforce_exporter_cx.py

run-aws-connect-ingestor:
	pipenv run python src/ingestors/aws_connect_ingestor.py

run-generic-ingestor:
	pipenv run python src/ingestors/generic_ingestor.py

run-salesforce-capacity:
	pipenv run python src/ingestors/salesforce_capacity.py

run-paid-ingestor:
	pipenv run python src/ingestors/paid_ingestor.py

run-adthena-ingestor:
	pipenv run python src/ingestors/ingest_adthena.py

run-MPE-converter:
	pipenv run python src/ingestors/MPEconverter.py

run-MPE2SF:
	pipenv run python src/ingestors/MPE2SF.py


run-field-history-pipeline:
#	pipenv run $(LINTER) $(LINTER_ARGS) src/etlkit
#	pipenv run $(TESTER) $(TESTER_ARGS) src/etlkit/tests
	pipenv run python src/etlkit/field_history_pipeline.py


#______ Data Visualisation Runners
run-CPA-dashboard:
	pipenv run python src/dataviz/CPA_dashboard.py

#===============================================================================


#===============================================================================
# CX dashboard runners
FILES = \
		src/ingestors/salesforce_exporter_cx.py \
		src/ingestors/aws_connect_ingestor.py \
		src/dataviz/cx_dashboard_v3.py
lint-cx-dashboard:
	pipenv run $(LINTER) $(LINTER_ARGS) $(FILES)

typecheck-cx-dashboard:
	pipenv run mypy $(FILES)

test-cx-dashboard:
	pipenv run $(TESTER) $(TESTER_ARGS) src/etlkit/tests
	pipenv run $(TESTER) $(TESTER_ARGS) src/ingestors/salesforce_exporter_cx_test.py
	pipenv run $(TESTER) $(TESTER_ARGS) src/ingestors/aws_connect_ingestor_test.py
	pipenv run $(TESTER) $(TESTER_ARGS) src/dataviz/cx_dashboard_test.py

run-cx-dashboard-v3:
	pipenv run python src/ingestors/salesforce_exporter_cx.py
	pipenv run python src/ingestors/aws_connect_ingestor.py
	pipenv run python src/dataviz/cx_dashboard_v3.py

#===============================================================================
# Conversion runners
run-conversion-exporters:
	pipenv run $(LINTER) $(LINTER_ARGS) src/conversion_exporter_v2
	pipenv run mypy src/conversion_exporter_v2
	pipenv run $(TESTER) $(TESTER_ARGS) src/conversion_exporter_v2/tests
	pipenv run python src/conversion_exporter_v2/CPA_conversion_exporter.py



#===============================================================================
#
# Migrations
# (these are destructive, so use with caution)
migrate:
	find migrations/[0-9]*.py -exec pipenv run python {} \;

migrate-accounts:
	pipenv run python migrations/003_delete_salesforce_accounts.py
	pipenv run python migrations/004_create_salesforce_accounts.py

migrate-opportunities:
	pipenv run python migrations/001_delete_salesforce_opportunities.py
	pipenv run python migrations/002_create_salesforce_opportunities.py



#===============================================================================
#
# Initialisers (to reset data tables without deleting)
#
initialise-salesforce-opportunities:
	pipenv run python initialisers/initialise_salesforce_opportunities.py

initialise-salesforce-accounts:
	pipenv run python initialisers/initialise_salesforce_accounts.py

initialise-google-ads:
	pipenv run python initialisers/initialise_google_ads.py


#===============================================================================
#
# Re-builders
#
rebuild-salesforce-opportunities:
	make migrate-opportunities
	make initialise-salesforce-opportunities
	make run-salesforce-opportunities

rebuild-salesforce-accounts:
	make migrate-accounts
	make initialise-salesforce-accounts
	make run-salesforce-accounts

#
#===============================================================================
#
# Tests
#
DIR_TO_TEST = src/ingestors
COM_TEST = pipenv run $(TESTER) $(TESTER_ARGS) $(DIR_TO_TEST)/

run-test-MPEconverter:
	$(COM_TEST)MPEconverter_test.py

run-test-MPE2SF:
	$(COM_TEST)MPE2SF_test.py

run-test-ingestor-tools:
	pipenv run pytest --disable-warnings src/ingestors/ingestor_tools_test.py

run-test-salesforce-opportunities:
	pipenv run pytest --disable-warnings src/ingestors/salesforce_opportunities_test.py

run-test-paid-ingestor:
	pipenv run pytest --disable-warnings src/ingestors/paid_ingestor_test.py

run-test-salesforce-capacity:
	$(COM_TEST)salesforce_capacity_test.py

run-test-apply-ingestor_tools:
	$(COM_TEST)apply_ingestor_tools_test.py

run-test-define-env-variables:
	$(COM_TEST)define_env_variables_test.py


FILES_TO_TEST = \
	$(DIR_TO_TEST)/MPE2SF_test.py \
	$(DIR_TO_TEST)/MPEconverter_test.py \
	$(DIR_TO_TEST)/ingestor_tools_test.py  \
	$(DIR_TO_TEST)/apply_ingestor_tools_test.py \
	$(DIR_TO_TEST)/define_env_variables_test.py \
	$(DIR_TO_TEST)/salesforce_exporter_test.py \
	$(DIR_TO_TEST)/salesforce_exporter_cx_test.py \
	$(DIR_TO_TEST)/aws_connect_ingestor_test.py
#	$(DIR_TO_TEST)/paid_ingestor_test.py \
#	$(DIR_TO_TEST)/salesforce_opportunities_test.py
#	$(DIR_TO_TEST)/salesforce_capacity_test.py \

test-ingestors:
	pipenv run $(TESTER) $(TESTER_ARGS) $(FILES_TO_TEST)


#_____ Paid Optimisation Tests
test-paidopt:
	pipenv run $(TESTER) $(TESTER_ARGS) src/paid_opt


#_____ Data Visualisation Tests
test-dataviz:
	pipenv run $(TESTER) $(TESTER_ARGS) src/dataviz

#_____ ELTkit tests
test-etlkit:
	pipenv run $(TESTER) $(TESTER_ARGS) src/etlkit/tests


#===============================================================================
#
# Linting
#
DIR_TO_LINT = src/ingestors
FILES_TO_LINT = \
	$(DIR_TO_LINT)/MPE2SF.py \
	$(DIR_TO_LINT)/MPE2SF_test.py \
	$(DIR_TO_LINT)/MPEconverter.py \
	$(DIR_TO_LINT)/MPEconverter_test.py \
	$(DIR_TO_LINT)/salesforce_capacity.py \
	$(DIR_TO_LINT)/paid_ingestor.py \
	$(DIR_TO_LINT)/paid_ingestor_test.py \
	$(DIR_TO_LINT)/apply_ingestor_tools.py \
	$(DIR_TO_LINT)/salesforce_exporter.py \
	$(DIR_TO_LINT)/salesforce_exporter_test.py \
	$(DIR_TO_LINT)/salesforce_exporter_account.py \
	$(DIR_TO_LINT)/salesforce_exporter_opportunities.py \
	$(DIR_TO_LINT)/salesforce_exporter_cx.py \
	$(DIR_TO_LINT)/salesforce_exporter_cx_test.py \
	$(DIR_TO_LINT)/aws_connect_ingestor.py \
	$(DIR_TO_LINT)/aws_connect_ingestor_test.py

lint-all:
	pipenv run $(LINTER) $(LINTER_ARGS) $(FILES_TO_LINT)
	pipenv run $(LINTER) $(LINTER_ARGS) src/dataviz

lint-paid-opt:
	pipenv run $(LINTER) $(LINTER_ARGS) $(PAIDOPTDIR)

lint-dataviz:
	pipenv run $(LINTER) $(LINTER_ARGS) src/dataviz




