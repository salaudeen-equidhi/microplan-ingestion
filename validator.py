# validator.py - Microplan Excel/CSV Validation Engine
import os
import re
import pandas as pd
import yaml
from datetime import datetime
from collections import defaultdict
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows


class Validator:
    """Validates microplan Excel/CSV files for data quality issues."""

    def __init__(self, config_path='validation_config.yaml'):
        # Load configuration from YAML
        self.config = self._load_config(config_path)

        # Column patterns (from config)
        self.boundary_patterns = self._get_config_list(['column_mappings', 'boundary_columns'])
        self.facility_patterns = self._get_config_list(['column_mappings', 'facility_columns'])
        self.target_patterns = self._get_config_list(['column_mappings', 'target_columns'])
        self.user_patterns = self._get_config_list(['column_mappings', 'user_columns'])

        # Exclude patterns (from config)
        self.exclude_patterns = self._get_config_list(['exclude_patterns', 'exclude_from_uniqueness'])
        self.exclude_from_naming = self._get_config_list(['exclude_patterns', 'exclude_from_naming'])
        self.root_indicators = self._get_config_list(['exclude_patterns', 'root_indicators'])
        self.special_allowed = self._get_config_list(['validation_rules', 'special_characters', 'allowed_special_chars'])

        # Sheet detection patterns (from config)
        self.boundary_sheet_patterns = self._get_config_list(['sheet_config', 'single_file', 'boundary_sheet_patterns'])
        self.facility_sheet_patterns = self._get_config_list(['sheet_config', 'single_file', 'facility_sheet_patterns'])

        # Validation rules toggle (from config)
        self.rules_enabled = {
            'non_zero_targets': self._get_config_bool(['validation_rules', 'non_zero_targets', 'enabled']),
            'naming_convention': self._get_config_bool(['validation_rules', 'naming_convention', 'enabled']),
            'boundary_alignment': self._get_config_bool(['validation_rules', 'boundary_alignment', 'enabled']),
            'unique_names': self._get_config_bool(['validation_rules', 'unique_names', 'enabled']),
            'user_mapping': self._get_config_bool(['validation_rules', 'user_mapping', 'enabled']),
            'no_missing_entries': self._get_config_bool(['validation_rules', 'no_missing_entries', 'enabled']),
            'special_characters': self._get_config_bool(['validation_rules', 'special_characters', 'enabled']),
            'hierarchy_check': self._get_config_bool(['validation_rules', 'hierarchy_check', 'enabled'])
        }

        # Hierarchy check settings (from config)
        self.hierarchy_auto_detect_root = self._get_config_bool(['validation_rules', 'hierarchy_check', 'auto_detect_root'])
        self.hierarchy_root_threshold_rows = self._get_config_value(['validation_rules', 'hierarchy_check', 'root_threshold_rows'], 5)
        self.hierarchy_root_threshold_percent = self._get_config_value(['validation_rules', 'hierarchy_check', 'root_threshold_percent'], 0.1)

        # ==================== STATE ====================
        self.row_status = {}
        self.file_data = {}
        self.output_files = []

    def _load_config(self, config_path):
        """Load configuration from YAML file."""
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    return yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load config file '{config_path}': {e}")
        return {}

    def _get_config_list(self, keys):
        """Get a list value from config."""
        try:
            value = self.config
            for key in keys:
                value = value[key]
            return value if isinstance(value, list) else []
        except (KeyError, TypeError):
            return []

    def _get_config_bool(self, keys):
        """Get a boolean value from config."""
        try:
            value = self.config
            for key in keys:
                value = value[key]
            return bool(value)
        except (KeyError, TypeError):
            return True  # Default enabled

    def _get_config_value(self, keys, default=None):
        """Get any value from config."""
        try:
            value = self.config
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

    # ==================== HELPER METHODS ====================

    def find_cols(self, df, patterns):
        """Find columns matching any of the patterns."""
        return [c for c in df.columns if any(p in str(c).lower() for p in patterns)]

    def find_name_cols(self, df, patterns):
        """Find columns matching patterns but EXCLUDE type/level columns."""
        cols = []
        for c in df.columns:
            col_lower = str(c).lower()
            if any(p in col_lower for p in patterns):
                if not any(ex in col_lower for ex in self.exclude_patterns):
                    cols.append(c)
        return cols

    def find_name_cols_for_naming(self, df, patterns):
        """Find columns for naming convention check - EXCLUDE codes/IDs."""
        cols = []
        for c in df.columns:
            col_lower = str(c).lower()
            if any(p in col_lower for p in patterns):
                if not any(ex in col_lower for ex in self.exclude_patterns):
                    if not any(ex in col_lower for ex in self.exclude_from_naming):
                        cols.append(c)
        return cols

    def find_parent_col(self, df):
        """Find the parent column (parent_code, parent_id, parent, etc.)."""
        for c in df.columns:
            col_lower = str(c).lower()
            if 'parent' in col_lower:
                return c
        return None

    def is_csv(self, filepath):
        """Check if file is CSV."""
        return filepath.lower().endswith('.csv')

    def init_row_status(self, df, sheet):
        """Initialize row status tracking."""
        self.row_status[sheet] = {}
        for idx in df.index:
            self.row_status[sheet][idx] = {'status': 'PASS', 'errors': []}

    def mark_row_error(self, sheet, row, error_msg):
        """Mark a row as having an error."""
        if sheet in self.row_status and row in self.row_status[sheet]:
            self.row_status[sheet][row]['status'] = 'FAIL'
            self.row_status[sheet][row]['errors'].append(error_msg)

    # ==================== VALIDATION CHECKS ====================

    def check_non_zero(self, df, sheet):
        """Check that target values are non-zero and properly rounded."""
        if not self.rules_enabled['non_zero_targets']:
            return []

        issues = []
        for col in self.find_cols(df, self.target_patterns):
            for idx, val in df[col].items():
                if pd.notna(val):
                    try:
                        n = float(val)
                        if n == 0:
                            issues.append({
                                'rule': 'Non-Zero Targets',
                                'severity': 'error',
                                'sheet': sheet,
                                'column': col,
                                'row': idx + 2,
                                'value': val,
                                'message': 'Zero value'
                            })
                            self.mark_row_error(sheet, idx, f'Zero value in {col}')
                        elif n != round(n):
                            issues.append({
                                'rule': 'Non-Zero Targets',
                                'severity': 'warning',
                                'sheet': sheet,
                                'column': col,
                                'row': idx + 2,
                                'value': val,
                                'message': f'Not rounded (should be {round(n)})'
                            })
                    except:
                        pass
        return issues

    def check_naming(self, df, sheet):
        """Check naming convention - only for NAME columns, NOT code/ID columns."""
        if not self.rules_enabled['naming_convention']:
            return []

        issues = []

        # Get name columns (excluding codes/IDs)
        name_cols = (self.find_name_cols_for_naming(df, self.boundary_patterns) +
                     self.find_name_cols_for_naming(df, self.facility_patterns))

        names = [(c, i, str(v).strip()) for c in name_cols
                 for i, v in df[c].items() if pd.notna(v)]

        if names:
            cases = {'upper': 0, 'lower': 0, 'title': 0, 'mixed': 0}
            for _, _, n in names:
                if n.isupper():
                    cases['upper'] += 1
                elif n.islower():
                    cases['lower'] += 1
                elif n.istitle():
                    cases['title'] += 1
                else:
                    cases['mixed'] += 1

            dom = max(cases, key=cases.get)
            cnt = 0
            for c, i, n in names:
                if not (n.isupper() or n.islower() or n.istitle()) and cnt < 10:
                    issues.append({
                        'rule': 'Naming Convention',
                        'severity': 'warning',
                        'sheet': sheet,
                        'column': c,
                        'row': i + 2,
                        'value': n[:40],
                        'message': f'Inconsistent case (dominant: {dom})'
                    })
                    cnt += 1

        return issues

    def check_alignment(self, b_df, f_df, b_sheet, f_sheet):
        """Check that facility boundary_code exists in boundary file's code column."""
        if not self.rules_enabled['boundary_alignment']:
            return []

        issues = []

        # Find code column in boundary file (first column or column with 'code' in name)
        b_code_col = None
        for c in b_df.columns:
            if 'code' in str(c).lower() and 'parent' not in str(c).lower():
                b_code_col = c
                break
        if not b_code_col and len(b_df.columns) > 0:
            b_code_col = b_df.columns[0]

        # Find boundary_code column in facility file
        f_code_col = None
        for c in f_df.columns:
            col_lower = str(c).lower()
            if 'boundary' in col_lower and 'code' in col_lower:
                f_code_col = c
                break
        # Fallback: look for any column with 'boundary' in it
        if not f_code_col:
            for c in f_df.columns:
                if 'boundary' in str(c).lower():
                    f_code_col = c
                    break

        if not b_code_col or not f_code_col:
            return issues

        # Get all valid boundary codes
        valid_codes = set(b_df[b_code_col].dropna().astype(str).str.strip().unique())

        # Check each facility's boundary_code exists in boundary file
        for idx, val in f_df[f_code_col].items():
            if pd.notna(val):
                code_str = str(val).strip()
                if code_str and code_str not in valid_codes:
                    issues.append({
                        'rule': 'Boundary Alignment',
                        'severity': 'error',
                        'sheet': f_sheet,
                        'column': f_code_col,
                        'row': idx + 2,
                        'value': code_str[:40],
                        'message': f'Boundary code not found in {b_sheet}'
                    })
                    self.mark_row_error(f_sheet, idx, f'Invalid boundary_code: {code_str}')

        return issues

    def check_unique(self, df, sheet):
        """Check for duplicate names in boundary and facility columns."""
        if not self.rules_enabled['unique_names']:
            return []

        issues = []

        # Get columns
        boundary_cols = self.find_name_cols(df, self.boundary_patterns)
        facility_cols = self.find_name_cols(df, self.facility_patterns)
        parent_col = self.find_parent_col(df)

        # Check first column for duplicates if it's a code/ID
        first_col = df.columns[0] if len(df.columns) > 0 else None
        if first_col is not None:
            first_col_lower = str(first_col).lower()
            if any(p in first_col_lower for p in ['code', 'id', 'boundary', 'key']):
                vals = df[first_col].dropna().astype(str).str.strip()
                seen = {}
                for idx, val in vals.items():
                    if val in seen:
                        issues.append({
                            'rule': 'Unique Names',
                            'severity': 'error',
                            'sheet': sheet,
                            'column': first_col,
                            'row': idx + 2,
                            'value': val[:40],
                            'message': f'Duplicate code/ID (also in row {seen[val] + 2})'
                        })
                        self.mark_row_error(sheet, idx, f'Duplicate {first_col}: {val}')
                    else:
                        seen[val] = idx

        # Check boundary columns
        for i, col in enumerate(boundary_cols):
            if parent_col and parent_col in df.columns:
                # Use explicit parent column
                try:
                    grouped = df.groupby(parent_col)[col]
                    for parent_val, group in grouped:
                        vals = group.dropna().astype(str).str.strip()
                        seen = {}
                        for idx, val in vals.items():
                            if val in seen:
                                issues.append({
                                    'rule': 'Unique Names',
                                    'severity': 'error',
                                    'sheet': sheet,
                                    'column': col,
                                    'row': idx + 2,
                                    'value': val[:40],
                                    'message': f'Duplicate under parent "{parent_val}" (also row {seen[val] + 2})'
                                })
                                self.mark_row_error(sheet, idx, f'Duplicate {col} "{val}" under parent "{parent_val}"')
                            else:
                                seen[val] = idx
                except Exception:
                    pass
            elif i == 0:
                # First column - globally unique
                vals = df[col].dropna().astype(str).str.strip()
                seen = {}
                for idx, val in vals.items():
                    if val in seen:
                        issues.append({
                            'rule': 'Unique Names',
                            'severity': 'error',
                            'sheet': sheet,
                            'column': col,
                            'row': idx + 2,
                            'value': val[:40],
                            'message': f'Duplicate top-level boundary (also in row {seen[val] + 2})'
                        })
                        self.mark_row_error(sheet, idx, f'Duplicate {col}: {val}')
                    else:
                        seen[val] = idx
            else:
                # Sub-level - use previous column as parent
                prev_col = boundary_cols[i - 1]
                try:
                    grouped = df.groupby(prev_col)[col]
                    for parent_val, group in grouped:
                        vals = group.dropna().astype(str).str.strip()
                        seen = {}
                        for idx, val in vals.items():
                            if val in seen:
                                issues.append({
                                    'rule': 'Unique Names',
                                    'severity': 'error',
                                    'sheet': sheet,
                                    'column': col,
                                    'row': idx + 2,
                                    'value': val[:40],
                                    'message': f'Duplicate under "{parent_val}" (also row {seen[val] + 2})'
                                })
                                self.mark_row_error(sheet, idx, f'Duplicate {col} "{val}" under {prev_col} "{parent_val}"')
                            else:
                                seen[val] = idx
                except Exception:
                    pass

        # Check facility columns
        for fac_col in facility_cols:
            group_col = parent_col if parent_col else (boundary_cols[-1] if boundary_cols else None)

            if group_col and group_col in df.columns:
                try:
                    grouped = df.groupby(group_col)[fac_col]
                    for parent_val, group in grouped:
                        vals = group.dropna().astype(str).str.strip()
                        seen = {}
                        for idx, val in vals.items():
                            if val in seen:
                                issues.append({
                                    'rule': 'Unique Names',
                                    'severity': 'error',
                                    'sheet': sheet,
                                    'column': fac_col,
                                    'row': idx + 2,
                                    'value': val[:40],
                                    'message': f'Duplicate facility under "{parent_val}" (also row {seen[val] + 2})'
                                })
                                self.mark_row_error(sheet, idx, f'Duplicate facility "{val}" under "{parent_val}"')
                            else:
                                seen[val] = idx
                except Exception:
                    pass
            else:
                # Global uniqueness
                vals = df[fac_col].dropna().astype(str).str.strip()
                seen = {}
                for idx, val in vals.items():
                    if val in seen:
                        issues.append({
                            'rule': 'Unique Names',
                            'severity': 'error',
                            'sheet': sheet,
                            'column': fac_col,
                            'row': idx + 2,
                            'value': val[:40],
                            'message': f'Duplicate facility name (also in row {seen[val] + 2})'
                        })
                        self.mark_row_error(sheet, idx, f'Duplicate facility: {val}')
                    else:
                        seen[val] = idx

        return issues

    def check_users(self, df, sheet):
        """Check for duplicate phone/contact numbers."""
        if not self.rules_enabled['user_mapping']:
            return []

        issues = []
        phone_cols = [c for c in self.find_cols(df, self.user_patterns)
                      if any(p in str(c).lower() for p in ['mobile', 'phone', 'contact'])]

        for col in phone_cols:
            if col in df.columns:
                vals = df[col].dropna().astype(str).str.replace(r'[\s\-\(\)]', '', regex=True)
                seen = {}
                for idx, val in vals.items():
                    if val and val != '':
                        if val in seen:
                            issues.append({
                                'rule': 'User Mapping',
                                'severity': 'warning',
                                'sheet': sheet,
                                'column': col,
                                'row': idx + 2,
                                'value': val,
                                'message': f'Duplicate contact (also row {seen[val] + 2})'
                            })
                        else:
                            seen[val] = idx

        return issues

    def check_missing(self, df, sheet):
        """Check for missing/blank values in required fields."""
        if not self.rules_enabled['no_missing_entries']:
            return []

        issues = []
        check_cols = set()
        check_cols.update(self.find_name_cols(df, self.boundary_patterns))
        check_cols.update(self.find_name_cols(df, self.facility_patterns))
        check_cols.update(self.find_cols(df, self.target_patterns))

        # Also check any column with 'name' in it (generic name columns)
        for c in df.columns:
            col_lower = str(c).lower()
            if 'name' in col_lower and not any(ex in col_lower for ex in self.exclude_from_naming):
                check_cols.add(c)

        for col in check_cols:
            nulls = df[col].isna() | (df[col].astype(str).str.strip() == '')
            for idx in df[nulls].index:
                issues.append({
                    'rule': 'No Missing Entries',
                    'severity': 'error',
                    'sheet': sheet,
                    'column': col,
                    'row': idx + 2,
                    'value': 'BLANK',
                    'message': 'Missing value'
                })
                self.mark_row_error(sheet, idx, f'Missing value in {col}')

        return issues

    def check_special(self, df, sheet):
        """Check for special characters in boundary and facility names."""
        if not self.rules_enabled['special_characters']:
            return []

        issues = []
        pattern = f'[^a-zA-Z0-9{re.escape("".join(self.special_allowed))}]'
        cnt = 0

        # Check boundary columns, facility columns, AND generic name columns
        name_cols = set()
        name_cols.update(self.find_name_cols_for_naming(df, self.boundary_patterns))
        name_cols.update(self.find_name_cols_for_naming(df, self.facility_patterns))

        # Also check any column with 'name' in it (but not code/id columns)
        for c in df.columns:
            col_lower = str(c).lower()
            if 'name' in col_lower and not any(ex in col_lower for ex in self.exclude_from_naming):
                name_cols.add(c)

        for col in name_cols:
            for idx, val in df[col].items():
                if pd.notna(val) and cnt < 20:
                    s = str(val).strip()
                    chars = re.findall(pattern, s)
                    if chars:
                        issues.append({
                            'rule': 'Special Characters',
                            'severity': 'error',
                            'sheet': sheet,
                            'column': col,
                            'row': idx + 2,
                            'value': s[:40],
                            'message': f'Found: {list(set(chars))}'
                        })
                        self.mark_row_error(sheet, idx, f'Special characters {list(set(chars))} in {col}')
                        cnt += 1

        return issues

    def check_hierarchy(self, df, sheet):
        """Check that parent references are valid (parent exists in hierarchy)."""
        if not self.rules_enabled['hierarchy_check']:
            return []

        issues = []
        parent_col = self.find_parent_col(df)

        if not parent_col or parent_col not in df.columns:
            return issues  # No parent column found, skip check

        # Get all boundary name/code columns that could be parents
        boundary_cols = self.find_name_cols(df, self.boundary_patterns)

        # Also check first column if it's a code column
        first_col = df.columns[0] if len(df.columns) > 0 else None
        code_col = None
        if first_col:
            first_col_lower = str(first_col).lower()
            if any(p in first_col_lower for p in ['code', 'id', 'boundary', 'key']):
                code_col = first_col

        # Collect all valid parent values (from boundary columns and code column)
        valid_parents = set()

        if code_col:
            valid_parents.update(df[code_col].dropna().astype(str).str.strip().unique())

        for col in boundary_cols:
            valid_parents.update(df[col].dropna().astype(str).str.strip().unique())

        # Root indicators from config (e.g., '', 'root', 'null', 'na', 'mz', etc.)
        root_indicators_lower = [r.lower() for r in self.root_indicators]

        # Detect potential root parents dynamically:
        # If a parent value is used by many rows but doesn't exist in boundary codes,
        # it's likely the root/country level (e.g., "mz" for Mozambique)
        detected_roots = set()
        if self.hierarchy_auto_detect_root:
            parent_counts = df[parent_col].dropna().astype(str).str.strip().value_counts()
            threshold_rows = self.hierarchy_root_threshold_rows
            threshold_percent = self.hierarchy_root_threshold_percent
            for parent_val, count in parent_counts.items():
                if parent_val not in valid_parents and parent_val.lower() not in root_indicators_lower:
                    # If this parent is used by many rows, treat as root
                    if count > threshold_rows or count > len(df) * threshold_percent:
                        detected_roots.add(parent_val)

        # Check each parent reference
        for idx, parent_val in df[parent_col].items():
            if pd.notna(parent_val):
                parent_str = str(parent_val).strip()
                if parent_str and parent_str not in valid_parents:
                    # Skip if it's a known root indicator or detected root
                    if parent_str.lower() in root_indicators_lower:
                        continue
                    if parent_str in detected_roots:
                        continue

                    issues.append({
                        'rule': 'Hierarchy Check',
                        'severity': 'error',
                        'sheet': sheet,
                        'column': parent_col,
                        'row': idx + 2,
                        'value': parent_str[:40],
                        'message': f'Parent "{parent_str}" not found in hierarchy'
                    })
                    self.mark_row_error(sheet, idx, f'Invalid parent: {parent_str}')

        return issues

    # ==================== MAIN VALIDATION METHODS ====================

    def validate_df(self, df, sheet):
        """Run all validation checks on a dataframe."""
        self.init_row_status(df, sheet)
        issues = []
        issues.extend(self.check_non_zero(df, sheet))
        issues.extend(self.check_naming(df, sheet))
        issues.extend(self.check_unique(df, sheet))
        issues.extend(self.check_users(df, sheet))
        issues.extend(self.check_missing(df, sheet))
        issues.extend(self.check_special(df, sheet))
        issues.extend(self.check_hierarchy(df, sheet))
        return issues

    def read_file(self, filepath):
        """Read CSV or Excel file and return dict of {sheet_name: dataframe}."""
        if self.is_csv(filepath):
            df = pd.read_csv(filepath)
            fname = os.path.basename(filepath)
            return {fname: df}
        else:
            xls = pd.ExcelFile(filepath)
            result = {}
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                if not df.empty:
                    result[sheet] = df
            return result

    def validate_file(self, filepath, b_sheet=None, f_sheet=None):
        """Validate a single file and return issues."""
        issues = []
        b_df, f_df, b_name, f_name = None, None, None, None

        if filepath not in self.file_data:
            self.file_data[filepath] = {}

        try:
            fname = os.path.basename(filepath)
            sheets_data = self.read_file(filepath)

            for sheet, df in sheets_data.items():
                if df.empty:
                    continue

                label = f"{fname} - {sheet}" if not self.is_csv(filepath) else fname
                issues.extend(self.validate_df(df, label))

                # Add status columns
                df_with_status = df.copy()
                df_with_status['VALIDATION_STATUS'] = 'PASS'
                df_with_status['VALIDATION_ERRORS'] = ''

                if label in self.row_status:
                    for idx, status_info in self.row_status[label].items():
                        if idx in df_with_status.index:
                            df_with_status.loc[idx, 'VALIDATION_STATUS'] = status_info['status']
                            df_with_status.loc[idx, 'VALIDATION_ERRORS'] = '; '.join(status_info['errors'])

                self.file_data[filepath][sheet] = df_with_status

                # Detect boundary/facility sheets using config patterns
                sl = sheet.lower()
                if b_sheet and b_sheet.lower() == sl:
                    b_df, b_name = df, label
                elif f_sheet and f_sheet.lower() == sl:
                    f_df, f_name = df, label
                elif any(p in sl for p in self.boundary_sheet_patterns):
                    if b_df is None:
                        b_df, b_name = df, label
                elif any(p in sl for p in self.facility_sheet_patterns):
                    if f_df is None:
                        f_df, f_name = df, label

            # Run alignment check if both sheets found
            if b_df is not None and f_df is not None:
                issues.extend(self.check_alignment(b_df, f_df, b_name, f_name))

        except Exception as e:
            issues.append({
                'rule': 'File Error',
                'severity': 'error',
                'sheet': filepath,
                'column': '-',
                'row': '-',
                'value': '-',
                'message': str(e)
            })

        return issues, self.summarize(issues)

    def summarize(self, issues):
        """Generate summary statistics from issues list."""
        summary = {
            'total': len(issues),
            'errors': len([i for i in issues if i['severity'] == 'error']),
            'warnings': len([i for i in issues if i['severity'] == 'warning']),
            'by_rule': defaultdict(int)
        }
        for i in issues:
            summary['by_rule'][i['rule']] += 1
        return summary

    def save_validated_files(self, output_folder='error'):
        """Save validated files with status columns and colors to output folder."""
        self.output_files = []
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Color definitions
        green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')  # Light green
        red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')    # Light red
        header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid') # Blue header
        header_font = Font(bold=True, color='FFFFFF')

        for filepath, sheets_data in self.file_data.items():
            if not sheets_data:
                continue

            base_name = os.path.basename(filepath)
            name_part = os.path.splitext(base_name)[0]

            if self.is_csv(filepath):
                # For CSV: Save as Excel with colors instead (better output)
                output_name = f"{name_part}_VALIDATED_{ts}.xlsx"
                output_path = os.path.join(output_folder, output_name)

                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    for sheet_name, df in sheets_data.items():
                        clean_name = sheet_name[:31] if len(sheet_name) > 31 else 'Sheet1'
                        df.to_excel(writer, sheet_name=clean_name, index=False)

                    # Apply colors after writing
                    workbook = writer.book
                    for sheet_name in workbook.sheetnames:
                        self._apply_colors(workbook[sheet_name], header_fill, header_font, green_fill, red_fill)

                self.output_files.append(output_path)
            else:
                output_name = f"{name_part}_VALIDATED_{ts}.xlsx"
                output_path = os.path.join(output_folder, output_name)

                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    for sheet_name, df in sheets_data.items():
                        clean_name = sheet_name[:31]
                        df.to_excel(writer, sheet_name=clean_name, index=False)

                    # Apply colors after writing
                    workbook = writer.book
                    for sheet_name in workbook.sheetnames:
                        self._apply_colors(workbook[sheet_name], header_fill, header_font, green_fill, red_fill)

                self.output_files.append(output_path)

        return self.output_files

    def _apply_colors(self, ws, header_fill, header_font, green_fill, red_fill):
        """Apply colors to VALIDATION_STATUS column only."""
        # Find VALIDATION_STATUS column
        status_col = None
        for col_idx, cell in enumerate(ws[1], 1):
            if cell.value == 'VALIDATION_STATUS':
                status_col = col_idx
            # Style header row
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center')

        if not status_col:
            return

        # Apply colors only to VALIDATION_STATUS cell
        for row_idx in range(2, ws.max_row + 1):
            status_cell = ws.cell(row=row_idx, column=status_col)
            if status_cell.value == 'PASS':
                status_cell.fill = green_fill
                status_cell.font = Font(bold=True, color='006100')  # Dark green text
            elif status_cell.value == 'FAIL':
                status_cell.fill = red_fill
                status_cell.font = Font(bold=True, color='9C0006')  # Dark red text

    def get_stats(self):
        """Get pass/fail row statistics."""
        pass_count = sum(1 for sheet_data in self.row_status.values()
                         for row_data in sheet_data.values() if row_data['status'] == 'PASS')
        fail_count = sum(1 for sheet_data in self.row_status.values()
                         for row_data in sheet_data.values() if row_data['status'] == 'FAIL')
        return pass_count, fail_count

    def reset(self):
        """Reset validator state for new validation."""
        self.row_status = {}
        self.file_data = {}
        self.output_files = []
