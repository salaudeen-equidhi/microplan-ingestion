import os
import glob
import base64
import ipywidgets as widgets
from IPython.display import display, HTML
from utils.facility_prep import generate_hf_district_mapping, fill_parent_codes
from utils.api import (
    ingest_facility,
    verify_facility_names,
    read_facility_names_from_csv,
    generate_facility_ingestion_summary,
)


def build_facilityingest_ui(ctx):
    OUTPUT_DIR = ctx['OUTPUT_DIR']
    config_state = ctx['config_state']
    UPLOADS_DIR = ctx['UPLOADS_DIR']
    ERROR_DIR = ctx['ERROR_DIR']

    # --- Derive boundary level names from config ---
    level_names = [col for col in config_state.get('level_columns', []) if col]

    # ====== Section A: Prepare Parent Codes ======

    facility_dropdown = widgets.Dropdown(
        options=[('Click Refresh to scan', '')],
        description='Facility CSV:',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='600px'),
    )

    facility_manual = widgets.Text(
        value='',
        placeholder='or type facility CSV file path here',
        description='',
        layout=widgets.Layout(width='600px'),
    )

    boundary_dropdown = widgets.Dropdown(
        options=[('Click Refresh to scan', '')],
        description='Boundary CSV:',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='600px'),
    )

    boundary_manual = widgets.Text(
        value='',
        placeholder='or type boundary CSV file path here',
        description='',
        layout=widgets.Layout(width='600px'),
    )

    refresh_btn = widgets.Button(
        description='Refresh', icon='refresh',
        layout=widgets.Layout(width='100px'))

    def scan_csv_files():
        patterns = [
            os.path.join(UPLOADS_DIR, '*.csv'),
            os.path.join(OUTPUT_DIR, 'csv_export_*', '*.csv'),
            os.path.join(OUTPUT_DIR, 'facility_build', '*.csv'),
        ]
        files = []
        for p in patterns:
            files.extend(glob.glob(p))
        return sorted(files)

    def on_refresh(btn):
        files = scan_csv_files()
        if files:
            opts = [('-- select --', '')] + [(os.path.basename(f), f) for f in files]
        else:
            opts = [('No CSV files found', '')]
        facility_dropdown.options = opts
        boundary_dropdown.options = opts
        facility_dropdown.value = ''
        boundary_dropdown.value = ''

    refresh_btn.on_click(on_refresh)

    state_code_input = widgets.Text(
        value='',
        description='State Code:',
        placeholder='Boundary code of the State (from boundary CSV)',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='500px'),
    )

    hf_parent_level = widgets.Dropdown(
        options=level_names if level_names else ['(configure levels first)'],
        description='Lowest Level:',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='400px'),
    )

    district_level = widgets.Dropdown(
        options=level_names if level_names else ['(configure levels first)'],
        description='District Level:',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='400px'),
    )

    hf_type_input = widgets.Text(
        value='Health Facility',
        description='HF Facility Type:',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='400px'),
    )

    district_type_input = widgets.Text(
        value='District Facility',
        description='District Facility Type:',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='400px'),
    )

    generate_btn = widgets.Button(
        description='Generate Parent Codes',
        button_style='success',
        icon='cogs',
    )
    prep_output = widgets.Output()

    # Track the prepared CSV path for Section B
    prepared_csv_path = {'value': ''}

    def on_generate(btn):
        prep_output.clear_output()
        with prep_output:
            facility_path = facility_manual.value.strip() or facility_dropdown.value
            if not facility_path or not os.path.isfile(facility_path):
                display(HTML('<p style="color:red">Please select or enter the facility CSV file path.</p>'))
                return

            boundary_path = boundary_manual.value.strip() or boundary_dropdown.value
            if not boundary_path or not os.path.isfile(boundary_path):
                display(HTML('<p style="color:red">Please select or enter the boundary CSV file path.</p>'))
                return

            state_code = state_code_input.value.strip()
            if not state_code:
                display(HTML('<p style="color:red">Please enter the state boundary code.</p>'))
                return

            hf_parent = hf_parent_level.value
            district = district_level.value
            hf_type = hf_type_input.value.strip()
            district_type = district_type_input.value.strip()

            display(HTML('<p>Step 1: Linking facilities to their districts...</p>'))
            try:
                mapping = generate_hf_district_mapping(boundary_path, hf_parent, district)
                display(HTML(f'<p>Linked <b>{len(mapping)}</b> facilities to their districts.</p>'))
            except Exception as e:
                display(HTML(f'<p style="color:red"><b>Error:</b> {e}</p>'))
                return

            upload_dir = os.path.join(OUTPUT_DIR, 'facility_prep')
            os.makedirs(upload_dir, exist_ok=True)
            output_name = os.path.basename(facility_path).rsplit('.', 1)[0] + '_prepared.csv'
            output_path = os.path.join(upload_dir, output_name)

            display(HTML('<p>Step 2: Filling parent codes in facility file...</p>'))
            try:
                updated = fill_parent_codes(
                    facility_path, output_path, mapping, state_code,
                    hf_type_name=hf_type, district_type_name=district_type,
                )
                prepared_csv_path['value'] = output_path
                with open(output_path, 'rb') as dl:
                    b64 = base64.b64encode(dl.read()).decode()
                display(HTML(
                    f'<p style="color:green"><b>Done!</b> Updated parent codes for <b>{updated}</b> facilities.</p>'
                    f'<p><a href="data:text/csv;base64,{b64}" download="{output_name}" '
                    f'style="display:inline-block; padding:8px 15px; background:#4caf50; color:white; '
                    f'text-decoration:none; border-radius:4px; margin:4px 0;">'
                    f'Download {output_name}</a></p>'
                ))
            except Exception as e:
                display(HTML(f'<p style="color:red"><b>Error:</b> {e}</p>'))

    refresh_btn.on_click(on_refresh)
    generate_btn.on_click(on_generate)

    # ====== Section B: Ingest ======

    api_url_input = widgets.Text(
        value='http://hcm-moz-impl.egov:8080/hcm-moz-impl/v1/dhis2/facilities/ingest',
        description='Server URL:',
        style={'description_width': '120px'},
        layout=widgets.Layout(width='600px'),
    )

    tenant_input = widgets.Text(
        value=config_state.get('tenant_id', ''),
        description='Tenant ID:',
        placeholder='e.g. bi',
        style={'description_width': '120px'},
        layout=widgets.Layout(width='300px'),
    )

    project_type_input = widgets.Text(
        value='',
        description='Project Type ID:',
        placeholder='from campaign setup',
        style={'description_width': '120px'},
        layout=widgets.Layout(width='500px'),
    )

    ingest_file_dropdown = widgets.Dropdown(
        options=[('Click Refresh to scan', '')],
        description='Select File:',
        style={'description_width': '120px'},
        layout=widgets.Layout(width='600px'),
    )

    ingest_manual = widgets.Text(
        value='',
        placeholder='or type prepared CSV file path here',
        description='',
        layout=widgets.Layout(width='600px'),
    )

    refresh_ingest_btn = widgets.Button(description='Refresh', icon='refresh')
    ingest_btn = widgets.Button(description='Upload to Server', button_style='info', icon='upload')
    ingest_output = widgets.Output()

    def scan_facility_csvs():
        """Scan for prepared facility CSVs."""
        prep_dir = os.path.join(OUTPUT_DIR, 'facility_prep')
        if not os.path.isdir(prep_dir):
            return []
        pattern = os.path.join(prep_dir, '*_prepared.csv')
        return sorted(glob.glob(pattern))

    def on_refresh_ingest(btn):
        files = scan_facility_csvs()
        if files:
            ingest_file_dropdown.options = [('-- select --', '')] + [
                (os.path.basename(f), f) for f in files
            ]
        else:
            ingest_file_dropdown.options = [('No prepared files found', '')]
        ingest_file_dropdown.value = ''

    def on_ingest(btn):
        ingest_output.clear_output()
        with ingest_output:
            url = api_url_input.value.strip()
            tenant = tenant_input.value.strip()
            project_type = project_type_input.value.strip()
            csv_path = ingest_manual.value.strip() or ingest_file_dropdown.value

            if not url:
                display(HTML('<p style="color:red">Please enter the server URL.</p>'))
                return
            if not tenant:
                display(HTML('<p style="color:red">Please enter the Tenant ID.</p>'))
                return
            if not project_type:
                display(HTML('<p style="color:red">Please enter the Project Type ID.</p>'))
                return
            if not csv_path:
                display(HTML('<p style="color:red">Please select a prepared facility file.</p>'))
                return
            if not os.path.isfile(csv_path):
                display(HTML(f'<p style="color:red">File not found: {csv_path}</p>'))
                return

            display(HTML(f'<p>Uploading <b>{os.path.basename(csv_path)}</b> to server...</p>'))
            try:
                result = ingest_facility(url, tenant, project_type, csv_path)
                if result['success']:
                    display(HTML(
                        f'<p style="color:green"><b>Upload successful!</b> (Status: {result["status_code"]})</p>'
                        f'<pre>{result["response"][:2000]}</pre>'
                    ))
                else:
                    display(HTML(
                        f'<p style="color:red"><b>Upload failed.</b> (Status: {result["status_code"]})</p>'
                        f'<pre>{result["response"][:2000]}</pre>'
                    ))
            except Exception as e:
                display(HTML(f'<p style="color:red"><b>Error:</b> {e}</p>'))

    refresh_ingest_btn.on_click(on_refresh_ingest)
    ingest_btn.on_click(on_ingest)

    # ====== Section C: Verify Facility Ingestion ======
    facility_search_url_input = widgets.Text(
        value='http://facility.egov:8080/facility/v1/_search?limit=1000&offset=0&tenantId=bi',
        description='Search URL:',
        style={'description_width': '120px'},
        layout=widgets.Layout(width='600px'),
    )

    verify_file_dropdown = widgets.Dropdown(
        options=[('Click Refresh to scan', '')],
        description='Facility CSV:',
        style={'description_width': '120px'},
        layout=widgets.Layout(width='600px'),
    )

    refresh_verify_btn = widgets.Button(description='Refresh', icon='refresh')
    verify_btn = widgets.Button(
        description='Verify Facility Ingestion',
        button_style='success',
        icon='check',
    )

    verify_progress_bar = widgets.IntProgress(
        value=0, min=0, max=1,
        description='Progress:',
        bar_style='info',
        style={'description_width': '80px'},
        layout=widgets.Layout(width='500px', visibility='hidden'),
    )
    verify_progress_label = widgets.HTML(value='')
    verify_progress_box = widgets.HBox([verify_progress_bar, verify_progress_label])
    verify_output = widgets.Output()

    report_dir = os.path.join(OUTPUT_DIR, 'facility_reports')

    def scan_verify_csvs():
        patterns = [
            os.path.join(OUTPUT_DIR, 'facility_prep', '*.csv'),
            os.path.join(OUTPUT_DIR, 'facility_build', '*.csv'),
            os.path.join(OUTPUT_DIR, 'csv_export_*', '*facilit*.csv'),
            os.path.join(UPLOADS_DIR, '*.csv'),
        ]
        files = []
        for p in patterns:
            files.extend(glob.glob(p))
        return sorted(set(files))

    def on_refresh_verify(btn):
        files = scan_verify_csvs()
        if files:
            verify_file_dropdown.options = [('-- select --', '')] + [
                (os.path.basename(f), f) for f in files
            ]
        else:
            verify_file_dropdown.options = [('No CSV files found', '')]
        verify_file_dropdown.value = ''

    def on_verify(btn):
        verify_output.clear_output()
        verify_progress_bar.layout.visibility = 'hidden'
        verify_progress_label.value = ''
        with verify_output:
            search_url = facility_search_url_input.value.strip()
            tenant = tenant_input.value.strip()
            csv_path = verify_file_dropdown.value

            if not search_url:
                display(HTML('<p style="color:red">Please enter the facility search URL.</p>'))
                return
            if not tenant:
                display(HTML('<p style="color:red">Please enter the Tenant ID in Section B.</p>'))
                return
            if not csv_path:
                display(HTML('<p style="color:red">Please select a facility CSV file.</p>'))
                return
            if not os.path.isfile(csv_path):
                display(HTML(f'<p style="color:red">File not found: {csv_path}</p>'))
                return

            try:
                name_col, unique_names = read_facility_names_from_csv(csv_path)
                if not unique_names:
                    display(HTML(f'<p style="color:red">No facility names found in column "{name_col}".</p>'))
                    return

                total = len(unique_names)
                verify_progress_bar.max = total
                verify_progress_bar.value = 0
                verify_progress_bar.bar_style = 'info'
                verify_progress_bar.layout.visibility = 'visible'
                verify_progress_label.value = f'<span style="margin-left:10px">Fetching facilities from server...</span>'

                def on_progress(current, total, name, found):
                    verify_progress_bar.value = current
                    status = '&#10003;' if found else '&#10007;'
                    verify_progress_label.value = (
                        f'<span style="margin-left:10px">Matching {current}/{total} '
                        f'&mdash; <code>{name}</code> {status}</span>'
                    )

                result = verify_facility_names(
                    search_url, tenant_id=tenant, facility_names=unique_names,
                    progress_cb=on_progress,
                )

                verify_progress_bar.bar_style = 'success'
                verify_progress_label.value = (
                    f'<span style="margin-left:10px; color:#2e7d32; font-weight:bold">'
                    f'Done &mdash; {total} facilities checked</span>'
                )

                found_names = result['found_names']
                not_found_names = result['not_found_names']
                found_count = len(found_names)
                not_found_count = len(not_found_names)
                rate = (found_count / total * 100) if total else 0

                summary = generate_facility_ingestion_summary(
                    csv_path=csv_path,
                    found_names=found_names,
                    output_dir=report_dir,
                    name_column=name_col,
                )

                with open(summary['output_path'], 'rb') as dl:
                    b64 = base64.b64encode(dl.read()).decode()
                output_name = os.path.basename(summary['output_path'])

                rate_color = '#2e7d32' if rate >= 90 else '#e65100' if rate >= 50 else '#c62828'

                display(HTML(
                    '<div style="padding:15px; background:#f5f5f5; border-radius:8px; '
                    'margin-top:10px; border-left:4px solid #1976d2;">'
                    '<h4 style="margin:0 0 10px 0">Facility Verification Summary</h4>'
                    '<table style="border-collapse:collapse; width:100%; max-width:400px">'
                    '<tr><td style="padding:4px 12px 4px 0">Total facilities</td>'
                    f'<td style="padding:4px 0; font-weight:bold">{total}</td></tr>'
                    '<tr><td style="padding:4px 12px 4px 0">Found in system</td>'
                    f'<td style="padding:4px 0; font-weight:bold; color:#2e7d32">{found_count}</td></tr>'
                    '<tr><td style="padding:4px 12px 4px 0">Not found in system</td>'
                    f'<td style="padding:4px 0; font-weight:bold; color:#c62828">{not_found_count}</td></tr>'
                    '<tr><td style="padding:4px 12px 4px 0">Success rate</td>'
                    f'<td style="padding:4px 0; font-weight:bold; color:{rate_color}">{rate:.1f}%</td></tr>'
                    '<tr><td style="padding:4px 12px 4px 0">Matched by</td>'
                    f'<td style="padding:4px 0"><code>{summary["name_column"]}</code></td></tr>'
                    '</table>'
                    f'<p style="margin:12px 0 0 0"><a href="data:application/vnd.openxmlformats-officedocument'
                    f'.spreadsheetml.sheet;base64,{b64}" '
                    f'download="{output_name}" style="display:inline-block; padding:8px 15px; background:#1976d2; '
                    f'color:white; text-decoration:none; border-radius:4px;">'
                    f'&#x2B07; Download {output_name}</a></p>'
                    '</div>'
                ))
            except Exception as e:
                verify_progress_bar.bar_style = 'danger'
                verify_progress_label.value = '<span style="margin-left:10px; color:#c62828">Failed</span>'
                display(HTML(f'<p style="color:red"><b>Error:</b> {e}</p>'))

    refresh_verify_btn.on_click(on_refresh_verify)
    verify_btn.on_click(on_verify)

    # ====== Layout ======
    help_text = """
    <div style="padding:10px; background:#f0f4ff; border-radius:5px; margin-bottom:10px">
    <b>Instructions:</b><br/>
    1. Select the facility CSV (with warehouse rows, UUIDs, and facility types already added)<br/>
    2. Select the boundary CSV exported earlier<br/>
    3. Enter the <b>state boundary code</b> from the boundary CSV<br/>
    4. Choose the <b>lowest level</b> (e.g. Village) and <b>district level</b> (e.g. District) from your boundary hierarchy<br/>
    5. Click <b>Generate Parent Codes</b> to auto-fill parent codes for health and district facilities<br/>
    6. Then use Section B to upload the prepared file to the server
    </div>
    """

    return widgets.VBox([
        widgets.HTML('<h3>Facility Ingestion</h3>'),
        widgets.HTML(help_text),

        # Section A
        widgets.HTML('<h4>A. Prepare Facility File</h4>'),
        widgets.HTML('<b>Facility CSV</b> <i>(with warehouse rows, UUIDs, and facility types added)</i>'),
        widgets.HBox([facility_dropdown, refresh_btn]),
        facility_manual,
        widgets.HTML('<b>Boundary CSV</b> <i>(exported in the previous step)</i>'),
        boundary_dropdown,
        boundary_manual,
        state_code_input,
        widgets.HBox([hf_parent_level, district_level]),
        widgets.HBox([hf_type_input, district_type_input]),
        generate_btn,
        prep_output,

        widgets.HTML('<hr/>'),

        # Section B
        widgets.HTML('<h4>B. Upload Facility to Server</h4>'),
        api_url_input,
        tenant_input,
        project_type_input,
        widgets.HBox([ingest_file_dropdown, refresh_ingest_btn]),
        ingest_manual,
        ingest_btn,
        ingest_output,

        widgets.HTML('<hr/>'),

        # Section C
        widgets.HTML('<h4>C. Verify Facility Ingestion</h4>'),
        facility_search_url_input,
        widgets.HBox([verify_file_dropdown, refresh_verify_btn]),
        verify_btn,
        verify_progress_box,
        verify_output,
    ])
