import datetime
import os
import re
import yaml
import fair_data_pipeline.fdp_utils as fdp_utils
import fair_data_pipeline.initialise_pipeline as initialise_pipeline

def initialise(token: str, config: str, script: str):
    return initialise_pipeline.initialise(token, config, script)


