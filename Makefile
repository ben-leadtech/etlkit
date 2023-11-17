
# variables, targets, dependencies, and recipes
TESTER = pytest
TESTER_ARGS = -ra --verbose --disable-warnings
LINTER = flake8
LINTER_ARGS = --verbose


lint:
	pipenv run $(LINTER) $(LINTER_ARGS) src

typecheck:
	pipenv run mypy src

tests:
	pipenv run $(TESTER) $(TESTER_ARGS) src
