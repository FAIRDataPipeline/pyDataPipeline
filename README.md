# pythonFDP

## Installation

```
git clone https://github.com/FAIRDataPipeline/pythonFDP.git

git checkout dev

pip install -e .
```

## Example submission_script

Assume FDP_CONFIG_DIR, storage_locations and objects have been set by CLI tool

```
from fair_data_pipeline.pyfdp import PyFDP

pipeline = PyFDP()
pipeline.token = 'YOUR_TOKEN_HERE'

config_path = os.environ.get('FDP_CONFIG_DIR') + '/config.yaml'
script_path = os.environ.get('FDP_CONFIG_DIR') + '/script.sh'

handle = pipeline.initialise(config_path, script_path)
pipeline.finalise(handle)
