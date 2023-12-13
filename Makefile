
# variables, targets, dependencies, and recipes
TESTER = pytest
TESTER_ARGS = --verbose --disable-warnings
LINTER = flake8
LINTER_ARGS = --verbose


lint:
	$(LINTER) $(LINTER_ARGS) etlkit tests

typecheck:
	mypy src tests

test:
	$(TESTER) $(TESTER_ARGS) tests
