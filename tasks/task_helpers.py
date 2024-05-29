import os
import yaml
import json
from semproc.yaml_configs import import_yaml_configs
# from tasks.solr_tasks import SolrBulkJob

'''
general task methods

mainly, parse the yaml configuration file
'''


def parse_yaml(yaml_file):
    with open(yaml_file, 'r') as f:
        y = yaml.safe_load(f.read())
    return y


def extract_task_config(yaml_config, task):
    return yaml_config.get(task, {})


def run_init(config):
    init_tasks = config.get('init', {})
    if not init_tasks:
        return


def read_data(path, is_json=True):
    with open(path, 'r') as f:
        if is_json:
            return json.loads(f.read())
        return f.read()


def write_data(path, data):
    with open(path, 'w') as f:
        f.write(json.dumps(data, indent=4))


def generate_output_filename(
        input_file, output_path, postfix, extension_overwrite=''):
    file_name, file_ext = os.path.splitext(os.path.basename(input_file))
    file_ext = extension_overwrite if extension_overwrite else file_ext
    return os.path.join(output_path, '_'.join([file_name, postfix]) + file_ext)


def clear_directory(directory):
    for d in os.walk(directory):
        subdir = d[0]
        files = d[2]
        for f in files:
            os.remove(os.path.join(subdir, f))


def load_yamls(yamls):
    return import_yaml_configs(yamls)
