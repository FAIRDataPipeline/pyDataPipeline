# pyDataPipeline

## Installation

```
git clone https://github.com/FAIRDataPipeline/pythonFDP.git

git checkout dev

pip install -e .
```
**NB. PyDataPipeline requires Python3.**

## Example submission_script

Assume FDP_CONFIG_DIR, storage_locations and objects have been set by CLI tool

```
import org.fairdatapipeline.api as pipeline

token_path = "/Users/ryanfield/.fair/registry/token"
config_path = os.environ.get('FDP_CONFIG_DIR') + '/config.yaml'
script_path = os.environ.get('FDP_CONFIG_DIR') + '/script.sh'

handle = pipeline.initialise(token_path, config_path, script_path)

pipeline.finalise(token_path, handle)

```