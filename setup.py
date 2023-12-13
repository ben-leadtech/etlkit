from setuptools import setup, find_packages

# Get the requirements from the requirements.txt file
with open('requirements.txt') as f:
	requirements = f.read().splitlines()

setup(
	name = 'etlkit',
	version = '0.0.1',
	author = 'Ben Davies',
	packages = find_packages(),
	install_requires = requirements,
	classifiers = [
		'Programming Language :: Python :: 3',
		'License :: OSI Approved :: MIT License',
		'Operating System :: OS Independent'
	]
)
