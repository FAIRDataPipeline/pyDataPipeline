from setuptools import setup, find_packages

setup(
    name='data-pipeline-api',
    version='0.4.0',
    author='SCRC / FAIR',
    description='Python FAIR data pipeline API (DPAPI)',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=[
        'PyYAML>=5.4.1',
        'requests>=2.26.0',
        'scipy',
        'h5py>=3.4.0',
        'matplotlib'
        ]
)
