import subprocess, sys, os, shutil, importlib

if os.path.exists('requirements.txt'):
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '-r', 'requirements.txt'], capture_output=True)

import validator as validator_module
importlib.reload(validator_module)
from validator import Validator

# Single output folder for everything — cleared on every re-run
OUTPUT_DIR = 'output'
UPLOADS_DIR = os.path.join(OUTPUT_DIR, 'uploads')
ERROR_DIR = os.path.join(OUTPUT_DIR, 'error')

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)

for folder in [UPLOADS_DIR, ERROR_DIR]:
    os.makedirs(folder, exist_ok=True)

validator = Validator()

# Apply name casing from config (default: lower)
from utils.common import set_casing_mode
casing_mode = validator.config.get('name_casing', 'lower')
set_casing_mode(casing_mode)

# Shared context for all UI cells
ctx = {
    'config_state': {
        'level_columns': [], 'target_columns': [], 'num_targets': 0,
        'facility_col': '', 'district_col': '', 'state_col': '',
        'alignment_mapping': {}, 'casing_mode': casing_mode, 'configured': False
    },
    'file_state': {'boundary_file': None, 'facility_file': None},
    'validator': validator,
    'OUTPUT_DIR': OUTPUT_DIR,
    'UPLOADS_DIR': UPLOADS_DIR,
    'ERROR_DIR': ERROR_DIR,
    'widgets': {},
}
