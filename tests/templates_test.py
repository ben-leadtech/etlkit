import pytest
from typing import Callable
from src.templates import generic_check_handler, generic_checker
from src.templates import ExtractTemplate, TransformTemplate, LoadTemplate


#=====================================================================#
class TestCheckersAndHandlers:
	tests: list = [
		(1==1, "Pass", ValueError),
		(1==2, "Fail", KeyError)
		]

	def test_checker(self):
		tests = self.tests
		assert generic_checker(*tests[0]) is None
		assert generic_checker(*tests[1]) == KeyError

	def test_handler(self):
		with pytest.raises(Exception):
			generic_check_handler(self.tests)
#=====================================================================#

#=====================================================================#
def test_ETLTemplates():
	templates = [ExtractTemplate, TransformTemplate, LoadTemplate]
	for template in templates:
		class Template(template):
			pass
		temp = Template()
		assert hasattr(temp,'run')
		assert isinstance(temp.run, Callable)
