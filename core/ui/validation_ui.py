import os
import glob
import base64
from collections import defaultdict
import ipywidgets as widgets
from IPython.display import display, HTML, clear_output


def build_validation_ui(ctx):
    config_state = ctx['config_state']
    file_state = ctx['file_state']
    validator = ctx['validator']
    UPLOADS_DIR = ctx['UPLOADS_DIR']
    ERROR_DIR = ctx['ERROR_DIR']

    out_status = widgets.Output()
    out_results = widgets.Output()
    out_downloads = widgets.Output()

    boundry_dropdown = widgets.Dropdown(
        options=[('Referesh to scan the file', '')],
        description='Select the boundary file',
        style={"description_width": '130px'})

    facility_dropdown = widgets.Dropdown(
        options=[('Referesh to scan the file', '')],
        description='Select the facility file',
        style={'description_width': '130px'})

    boundry_manual_path = widgets.Text(
        description='', value=file_state.get('boundary_file') or '',
        placeholder='or manually type boundary file path')

    facility_manual_path = widgets.Text(
        description='', value=file_state.get('facility_file') or '',
        placeholder='or manually type facility file path')

    def scan_csv_file():
        csv_file = []
        path = [
            os.path.join(UPLOADS_DIR, '*.csv'),
            os.path.join(UPLOADS_DIR, '*.xls'),
            os.path.join(UPLOADS_DIR, '*.xlsx'),
        ]
        for pattern in path:
            csv_file.extend(glob.glob(pattern))
        return csv_file

    def refresh_list(d=None):
        csv_file = scan_csv_file()
        if csv_file:
            boundry_dropdown.options = [('select the csv file', None)] + [(f, f) for f in csv_file]
            facility_dropdown.options = [('select the csv file', None)] + [(f, f) for f in csv_file]
        else:
            boundry_dropdown.options = [('No csv file found', None)]
            facility_dropdown.options = [('No csv file found', None)]
        facility_dropdown.value = None
        boundry_dropdown.value = None

    upload1 = widgets.FileUpload(accept='.xlsx,.xls,.csv', multiple=False)
    upload2 = widgets.FileUpload(accept='.xlsx,.xls,.csv', multiple=False)

    btn_validate = widgets.Button(
        description='VALIDATE', button_style='primary',
        layout=widgets.Layout(width='120px'))
    btn_clear = widgets.Button(
        description='Clear', button_style='warning',
        layout=widgets.Layout(width='80px'))

    def show_status(msg, color='black'):
        with out_status:
            clear_output(wait=True)
            display(HTML(f'<p style="color:{color}; font-weight:bold">{msg}</p>'))

    def save_upload(uploader, key):
        if not uploader.value:
            return
        files = uploader.value
        info = files[0] if isinstance(files, tuple) else list(files.values())[0]
        name = info.name if hasattr(info, 'name') else info['name']
        content = info.content if hasattr(info, 'content') else info['content']
        path = os.path.join(UPLOADS_DIR, name)
        with open(path, 'wb') as f:
            f.write(content)
        file_state[key] = path
        show_status(f'Loaded: {name}', 'green')

    def on_validate(b):
        boundary_path = (
            boundry_manual_path.value.strip()
            or boundry_dropdown.value
            or file_state['boundary_file']
        )
        facility_path = (
            facility_manual_path.value.strip()
            or facility_dropdown.value
            or file_state['facility_file']
        )
        if not boundary_path:
            show_status('Upload or select boundary file!', 'red')
            return
        if not facility_path:
            show_status('Upload or select facility file!', 'red')
            return

        file_state['boundary_file'] = boundary_path
        file_state['facility_file'] = facility_path

        validator.reset()

        # Skip naming convention check if auto-fix casing is enabled
        if config_state.get('casing_mode', 'none') != 'none':
            validator.rules_enabled['naming_convention'] = False

        show_status('Validating...', 'blue')
        all_issues = []
        summary = {'total': 0, 'errors': 0, 'warnings': 0, 'by_rule': defaultdict(int)}

        # Validate boundary file
        validator.set_columns(
            boundary_cols=config_state['level_columns'],
            facility_cols=[config_state['facility_col']] if config_state['facility_col'] else [],
            target_cols=config_state['target_columns'], num_targets=config_state['num_targets'])

        issues, s = validator.validate_file(file_state['boundary_file'])
        all_issues.extend(issues)
        summary['total'] += s['total']
        summary['errors'] += s['errors']
        summary['warnings'] += s['warnings']
        for r, c in s['by_rule'].items():
            summary['by_rule'][r] += c

        # Get boundary data for alignment
        b_sheets = validator.read_file(file_state['boundary_file'])
        b_sheet = list(b_sheets.keys())[0]
        b_df = b_sheets[b_sheet]

        # Validate facility file
        fac_cols = [c for c in [config_state['facility_col'], config_state['district_col'], config_state['state_col']] if c]
        validator.set_columns(
            boundary_cols=fac_cols,
            facility_cols=[config_state['facility_col']] if config_state['facility_col'] else [],
            target_cols=[], num_targets=0)
        validator.set_alignment_mapping(config_state['alignment_mapping'])

        issues2, s2 = validator.validate_file(file_state['facility_file'])
        all_issues.extend(issues2)
        summary['total'] += s2['total']
        summary['errors'] += s2['errors']
        summary['warnings'] += s2['warnings']
        for r, c in s2['by_rule'].items():
            summary['by_rule'][r] += c

        # Run alignment check
        if config_state['alignment_mapping']:
            f_sheets = validator.read_file(file_state['facility_file'])
            f_sheet = list(f_sheets.keys())[0]
            f_df = f_sheets[f_sheet]
            f_label = os.path.basename(file_state['facility_file'])

            if f_label not in validator.row_status:
                validator.init_row_status(f_df, f_label)

            align_issues = validator.check_alignment(b_df, f_df, b_sheet, f_label)
            all_issues.extend(align_issues)
            summary['total'] += len(align_issues)
            summary['errors'] += len([i for i in align_issues if i['severity'] == 'error'])
            for i in align_issues:
                summary['by_rule'][i['rule']] += 1

            if file_state['facility_file'] in validator.file_data:
                for sn, df in validator.file_data[file_state['facility_file']].items():
                    if f_label in validator.row_status:
                        for idx, info in validator.row_status[f_label].items():
                            if idx in df.index:
                                # Only upgrade status to FAIL, never downgrade FAIL to PASS
                                if info['status'] == 'FAIL':
                                    df.loc[idx, 'VALIDATION_STATUS'] = 'FAIL'
                                existing_errors = str(df.loc[idx, 'VALIDATION_ERRORS']).strip()
                                new_errors = '; '.join(info['errors'])
                                if existing_errors and existing_errors != 'nan' and new_errors:
                                    df.loc[idx, 'VALIDATION_ERRORS'] = existing_errors + '; ' + new_errors
                                elif new_errors:
                                    df.loc[idx, 'VALIDATION_ERRORS'] = new_errors

        output_files = validator.save_validated_files(ERROR_DIR)
        display_results(all_issues, summary)
        display_downloads(output_files)
        show_status('Done!', 'green')

    def on_clear(b):
        file_state['boundary_file'] = None
        file_state['facility_file'] = None
        validator.reset()
        with out_results:
            clear_output()
        with out_downloads:
            clear_output()
        show_status('Cleared', 'orange')

    def display_results(issues, summary):
        with out_results:
            clear_output(wait=True)
            p, f = validator.get_stats()
            color = '#27ae60' if summary['errors'] == 0 else '#e74c3c'

            html = f'''<div style="padding:10px; background:#f0f0f0; border-left:4px solid {color}; margin:10px 0;">
                <b style="color:{color}">{'All Passed!' if summary['errors']==0 else 'Issues Found'}</b>
                | PASS: {p} | FAIL: {f} | Warnings: {summary['warnings']}</div>'''

            if summary['by_rule']:
                html += '<b>By Rule:</b> ' + ', '.join([f'{r}: {c}' for r, c in summary['by_rule'].items()])

            if issues:
                html += '<div style="max-height:250px; overflow-y:auto; margin-top:10px;">'
                html += '<table style="width:100%; font-size:11px; border-collapse:collapse;">'
                html += '<tr style="background:#333; color:white;"><th>Sev</th><th>Rule</th><th>Sheet</th><th>Col</th><th>Row</th><th>Value</th><th>Message</th></tr>'
                for i in issues[:50]:
                    c = '#c00' if i['severity'] == 'error' else '#d80'
                    html += f'<tr><td style="color:{c}">{i["severity"][:3].upper()}</td><td>{i["rule"]}</td>'
                    html += f'<td>{str(i["sheet"])[:20]}</td><td>{str(i["column"])[:12]}</td><td>{i["row"]}</td>'
                    html += f'<td>{str(i["value"])[:15]}</td><td>{i["message"]}</td></tr>'
                html += '</table></div>'
            display(HTML(html))

    def display_downloads(files):
        with out_downloads:
            clear_output(wait=True)
            for fp in files:
                name = os.path.basename(fp)
                with open(fp, 'rb') as f:
                    b64 = base64.b64encode(f.read()).decode()
                display(HTML(
                    f'<a href="data:application/octet-stream;base64,{b64}" download="{name}" '
                    f'style="display:inline-block; padding:8px 15px; background:#4caf50; color:white; '
                    f'text-decoration:none; border-radius:4px; margin:5px 0;">Download {name}</a>'))

    upload1.observe(lambda c: save_upload(upload1, 'boundary_file'), names='value')
    upload2.observe(lambda c: save_upload(upload2, 'facility_file'), names='value')
    btn_validate.on_click(on_validate)
    btn_clear.on_click(on_clear)

    referesh_btn = widgets.Button(
        value='', description='refresh', button_style='info')
    referesh_btn.on_click(refresh_list)

    return widgets.VBox([
        widgets.HTML('<h3>Upload & Validate</h3>'),
        widgets.HTML('<b>Boundary File:</b>'), upload1,
        boundry_dropdown,
        boundry_manual_path,
        widgets.HTML('<b>Facility File:</b>'), upload2,
        facility_dropdown,
        facility_manual_path,
        out_status,
        widgets.HBox([referesh_btn, btn_validate, btn_clear]),
        out_results, out_downloads
    ])
