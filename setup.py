from setuptools import setup, find_packages

setup(
    name='PyFDP',
    version='0.0.1',
    author='SCRC',
    description='Python FAIR data pipeline API',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'PyYAML==5.3.1',
        'requests==2.23.0',
        ]
)
