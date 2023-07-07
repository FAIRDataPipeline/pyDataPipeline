# pyDataPipeline

[![PyPI](https://img.shields.io/pypi/v/data-pipeline-api)](https://pypi.org/project/data-pipeline-api/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/data-pipeline-api)](https://pypi.org/project/data-pipeline-api/)
[![pyDataPipeline](https://github.com/FAIRDataPipeline/pyDataPipeline/actions/workflows/pyDataPipeline.yaml/badge.svg?branch=dev)](https://github.com/FAIRDataPipeline/pyDataPipeline/actions/workflows/pyDataPipeline.yaml)
[![codecov](https://codecov.io/gh/FAIRDataPipeline/pyDataPipeline/branch/dev/graph/badge.svg?token=Eax5AmrDxx)](https://codecov.io/gh/FAIRDataPipeline/pyDataPipeline)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5562602.svg)](https://doi.org/10.5281/zenodo.5562602)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/5461/badge)](https://bestpractices.coreinfrastructure.org/projects/5461)

Welcome to pyDataPipeline a Python api to interact with the Fair Data Pipeline.

Full documention of the pyDataPipeline is avaialable at [https://www.fairdatapipeline.org/pyDataPipeline/](https://www.fairdatapipeline.org/pyDataPipeline/)

## Installation
pyDataPipeline can be installed from PyPi:
```
pip3 install data-pipeline-api
```

Or from the Repository:
```
git clone https://github.com/FAIRDataPipeline/pythonFDP.git

git checkout dev

pip3 install -e .
```
**NB. PyDataPipeline requires Python3.**

## Example submission_script

Assume FDP_CONFIG_DIR, storage_locations and objects have been set by CLI tool

```
import os
import fairdatapipeline as pipeline

token = os.environ['FDP_LOCAL_TOKEN']
config_dir = os.environ['FDP_CONFIG_DIR']
config_path = os.path.join(config_dir, 'config.yaml')
script_path = os.path.join(config_dir, 'script.sh')

handle = pipeline.initialise(token, config_path, script_path)

pipeline.finalise(token, handle)

```

## SEIRS Model Example

The SEIRS Model Example is available at: [https://www.fairdatapipeline.org/pyDataPipeline/examples/SEIRS.html](https://www.fairdatapipeline.org/pyDataPipeline/examples/SEIRS.html)
