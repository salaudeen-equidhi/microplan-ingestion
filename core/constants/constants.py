from collections import OrderedDict

DATE_FORMAT = "%d/%m/%Y"

FACILITY_START_ROW = 1
START_BOUNDARIES_ROW = 1
BOUNDARY_1_CODE = "mz"
DB_CONNECTION_STRING = "sqlite:///output/microplan.db"

CHECKLIST_TARGETS = "{\"fields\":[]}"
PROJECT_NAME = "IRS"
LAST_BOUNDARY = "BOUNDARY_7"


BOUNDARIES = OrderedDict({
    "BOUNDARY_1": {"name": "COUNTRY", "level": 1, "code": BOUNDARY_1_CODE},
    "BOUNDARY_2": {"name": "Provincia", "level": 2, "column": "B"},
    "BOUNDARY_3": {"name": "Distrito", "level": 3, "column": "C"},
    "BOUNDARY_4": {"name": "Posto Administrativo", "level": 4, "column": "D"},
    "BOUNDARY_5": {"name": "Localidade", "level": 5, "column": "E"},
    "BOUNDARY_6": {"name": "Unidade Sanitaria", "level": 6, "column": "F"},
    "BOUNDARY_7": {"name": "Aldeia", "level": 7, "column": "G"}
})

TARGET_COLUMNS = {
    'target_1': 'H',
    'target_2': 'I',
    'target_3': 'J',
    'target_4': 'K',
    'target_5': 'L'
}

TOTAL_COLUMNS = {
    'total_1': 'H',
    'total_2': 'I',
    'total_3': 'J',
    'total_4': 'K',
    'total_5': 'L'
}


def get_boundary_name(boundary_level):
    for key, value in BOUNDARIES.items():
        if value.get("level") == boundary_level:
            return key
    return "Invalid level"


def get_boundary_info(boundary_key):
    return BOUNDARIES.get(boundary_key, {})


def get_boundary_code(boundary_key):
    return BOUNDARIES.get(boundary_key, {}).get("code", "")


class TransformConfig:
    """Builds config from notebook inputs and pushes it to module-level vars."""

    def __init__(self):
        self.config = {}

    @classmethod
    def from_notebook(cls, config_state, user_inputs):
        import shortuuid

        cfg = cls()
        level_columns = config_state.get('level_columns', [])
        target_columns = config_state.get('target_columns', [])
        boundary_columns = config_state.get('boundary_columns', {})
        target_column_letters = config_state.get('target_column_letters', {})

        country_code = user_inputs.get('country_code', 'mz')

        # boundary hierarchy
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

        # target columns
        tgt_cols = OrderedDict()
        if target_columns:
            if target_column_letters:
                for t_name in target_columns:
                    tgt_cols[t_name] = target_column_letters.get(t_name, '')
            else:
                start = chr(64 + len(level_columns) + 1)
                for i, t_name in enumerate(target_columns):
                    tgt_cols[t_name] = chr(ord(start) + i)

        # total columns (same letters as targets)
        total_cols = OrderedDict()
        for i, (_, col) in enumerate(tgt_cols.items(), start=1):
            total_cols[f"total_{i}"] = col

        province_code = user_inputs.get('province_code', '')
        if not province_code:
            province_code = str(shortuuid.uuid())

        # which boundary level facilities map to
        facility_col = config_state.get('facility_col', '')
        alignment = config_state.get('alignment_mapping', {})
        facility_maps_to = alignment.get(facility_col, '')
        facility_level = len(level_columns) if level_columns else 4
        if facility_maps_to and level_columns:
            for i, name in enumerate(level_columns):
                if name == facility_maps_to:
                    facility_level = i + 1
                    break

        cfg.config = {
            'BOUNDARY_1_CODE': country_code,
            'BOUNDARY_2_NAME': user_inputs.get('province_name', ''),
            'BOUNDARY_2_CODE': province_code,
            'DB_CONNECTION_STRING': f"sqlite:///{user_inputs.get('db_name', 'output/microplan.db')}",
            'PROJECT_NAME': user_inputs.get('project_name', 'IRS'),
            'BOUNDARIES': boundaries,
            'TARGET_COLUMNS': tgt_cols,
            'TOTAL_COLUMNS': total_cols,
            'LAST_BOUNDARY': f"BOUNDARY_{len(level_columns)}" if level_columns else "BOUNDARY_7",
            'START_BOUNDARIES_ROW': user_inputs.get('boundary_start_row', 1),
            'FACILITY_START_ROW': user_inputs.get('facility_start_row', 1),
            'CAMPAIGN_START_DATE': user_inputs.get('campaign_start_date', ''),
            'CAMPAIGN_END_DATE': user_inputs.get('campaign_end_date', ''),
            'FACILITY_BOUNDARY_LEVEL': facility_level,
        }

        return cfg

    def apply_to_module(self):
        import sys
        mod = sys.modules[__name__]
        pkg = sys.modules.get('constants')
        for key, value in self.config.items():
            setattr(mod, key, value)
            if pkg is not None:
                setattr(pkg, key, value)
