from collections import OrderedDict
BOUNDARY_3_CELL = "B2"
BOUNDARY_2_CODE = "f59850de767417d5e8378n"
BOUNDARY_2_NAME = "Niassa"

DATE_FORMAT = "%d/%m/%Y"
OUTPUT_PATH = "./files/output/data_templates"
START_HEALTH_FACILITY_COLUMN = "D"
START_HF_COLUMN = "D"
START_SETTLEMENT_COLUMN = "H"

HF_MP_FACILITY_COLUMN = "C"
FACILITY_START_ROW = 1
HF_CODE_START_ROW = 4

START_BOUNDARIES_ROW = 1
BOUNDARY_1_CODE = "mz"
DB_CONNECTION_STRING = "sqlite:///niassa-full-ingestion-20Jan-2026.db"

CHECKLIST_TARGETS = "{\"fields\":[]}"
BENEFICIARY_PER_CD = 260
PROJECT_NAME = "IRS"
FACILITY_CODE_START_ROW = 1
FACILITY_LEVEL_1 = "Level 1"
FACILITY_LEVEL_2 = "Level 2"
FACILITY_LEVEL_4 = "Level 4"
LAST_BOUNDARY = "BOUNDARY_7"
BOUNDARY_3_FACILITY_COLUMN = "S"
BOUNDARY_3_FACILITY_ROW = 2
BOUNDARY_3_NAME = "Tofa"
TARGET_FACILITY_END = 10


BOUNDARIES = OrderedDict({
    "BOUNDARY_1": {"name": "COUNTRY", "level": 1, "code": BOUNDARY_1_CODE},
    "BOUNDARY_2": {"name": "Provincia", "level": 2, "column": "B"},
    "BOUNDARY_3": {"name": "Distrito", "level": 3, "column": "C"},
    "BOUNDARY_4": {"name": "Posto Administrativo", "level": 4, "column": "D"},
    "BOUNDARY_5": {"name": "Localidade", "level": 5, "column": "E"},
    "BOUNDARY_6": {"name": "Unidade Sanitaria", "level": 6, "column": "F"},
    "BOUNDARY_7": {"name": "Aldeia", "level": 7, "column": "G"}
})

FACILITY_COLUMNS = {
    "BOUNDARY_3": 'N'
}

TARGET_COLUMNS = {
    'target_1': 'H',
    'target_2': 'I',
    'target_3': 'J',
    'target_4': 'K',
    'target_5': 'L'
}

FACILITY_TARGET_COLUMNS = [
    "target"
]
FACILITY_TARGET_COLUMNS = ['target_3', 'target_4', 'target_5', 'target_6',
                           'target_7', 'target_8', 'target_9', 'target_10', 'target_11', 'target_12']
FACILITY_TOTAL_COLUMNS = ['total_3', 'total_4', 'total_5', 'total_6',
                          'total_7', 'total_8', 'total_9', 'total_10', 'total_11', 'total_12']


TOTAL_COLUMNS = {
    'total_1': 'H',
    'total_2': 'I',
    'total_3': 'J',
    'total_4': 'K',
    'total_5': 'L'
}


def get_boundary_info(boundary_key):
    return BOUNDARIES.get(boundary_key, {})


def get_boundary_name(boundary_level):
    for key, value in BOUNDARIES.items():
        if value.get("level") == boundary_level:
            return key
    return "Invalid level"


def get_boundary_code(boundary_key):
    return BOUNDARIES.get(boundary_key, {}).get("code", "")


class TransformConfig:
    """Dynamic configuration builder for notebook integration."""

    def __init__(self):
        self.config = {}

    @classmethod
    def from_notebook(cls, config_state, user_inputs):
        """Build configuration from notebook inputs.

        Args:
            config_state: Dict from Cell 2 (level_columns, target_columns, etc.)
            user_inputs: Dict from Cell 5 (db_name, country_code, province_name, etc.)
        """
        import shortuuid

        cfg = cls()
        level_columns = config_state.get('level_columns', [])
        target_columns = config_state.get('target_columns', [])
        boundary_columns = config_state.get('boundary_columns', {})
        target_column_letters = config_state.get('target_column_letters', {})

        country_code = user_inputs.get('country_code', 'mz')

        # Build BOUNDARIES OrderedDict
        boundaries = OrderedDict()
        if level_columns:
            boundaries["BOUNDARY_1"] = {
                "name": level_columns[0],
                "level": 1,
                "code": country_code
            }
            for i, name in enumerate(level_columns[1:], start=2):
                col = boundary_columns.get(i, chr(64 + i))
                boundaries[f"BOUNDARY_{i}"] = {
                    "name": name,
                    "level": i,
                    "column": col
                }

        # Build TARGET_COLUMNS
        tgt_cols = OrderedDict()
        if target_columns:
            if target_column_letters:
                for t_name in target_columns:
                    tgt_cols[t_name] = target_column_letters.get(t_name, '')
            else:
                start = chr(64 + len(level_columns) + 1)
                for i, t_name in enumerate(target_columns):
                    tgt_cols[t_name] = chr(ord(start) + i)

        # Build TOTAL_COLUMNS (mirror target columns)
        total_cols = OrderedDict()
        for i, (_, col) in enumerate(tgt_cols.items(), start=1):
            total_cols[f"total_{i}"] = col

        # Province code
        province_code = user_inputs.get('province_code', '')
        if not province_code:
            province_code = str(shortuuid.uuid())

        cfg.config = {
            'BOUNDARY_1_CODE': country_code,
            'BOUNDARY_2_NAME': user_inputs.get('province_name', ''),
            'BOUNDARY_2_CODE': province_code,
            'DB_CONNECTION_STRING': f"sqlite:///{user_inputs.get('db_name', 'microplan.db')}",
            'PROJECT_NAME': user_inputs.get('project_name', 'IRS'),
            'BOUNDARIES': boundaries,
            'TARGET_COLUMNS': tgt_cols,
            'TOTAL_COLUMNS': total_cols,
            'LAST_BOUNDARY': f"BOUNDARY_{len(level_columns)}" if level_columns else "BOUNDARY_7",
            'START_BOUNDARIES_ROW': user_inputs.get('boundary_start_row', 1),
            'FACILITY_START_ROW': user_inputs.get('facility_start_row', 1),
            'CAMPAIGN_START_DATE': user_inputs.get('campaign_start_date', ''),
            'CAMPAIGN_END_DATE': user_inputs.get('campaign_end_date', ''),
        }

        return cfg

    def apply_to_module(self):
        """Push configuration to module-level variables."""
        import sys
        mod = sys.modules[__name__]
        pkg = sys.modules.get('constants')
        for key, value in self.config.items():
            setattr(mod, key, value)
            if pkg is not None:
                setattr(pkg, key, value)
