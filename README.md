# pyDataPipeline

[![pyDataPipeline](https://github.com/FAIRDataPipeline/pyDataPipeline/actions/workflows/pyDataPipeline.yaml/badge.svg?branch=dev)](https://github.com/FAIRDataPipeline/pyDataPipeline/actions/workflows/pyDataPipeline.yaml)
[![codecov](https://codecov.io/gh/FAIRDataPipeline/pyDataPipeline/branch/dev/graph/badge.svg?token=Eax5AmrDxx)](https://codecov.io/gh/FAIRDataPipeline/pyDataPipeline)

## Installation

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
import org.fairdatapipeline.api as pipeline

token = os.environ.get('FDP_LOCAL_TOKEN')
config_path = os.environ.get('FDP_CONFIG_DIR') + '/config.yaml'
script_path = os.environ.get('FDP_CONFIG_DIR') + '/script.sh'

handle = pipeline.initialise(token, config_path, script_path)

pipeline.finalise(token, handle)

```
