from setuptools import setup, find_packages

setup(
    name='PyFDP',
    version='0.0.1',
    author='SCRC',
    description='Python FAIR data pipeline API',
    packages=find_packages(),
    install_requires=['urllib', 'datetime', 'yaml', 'requests', 'json'],
)
