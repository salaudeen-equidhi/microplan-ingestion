import os
import glob
import base64
import ipywidgets as widgets
from IPython.display import display, HTML
from utils.api import (
    ingest_boundary,
    verify_boundary_codes,
    read_boundary_codes_from_csv,
    generate_boundary_ingestion_summary,
)


def build_boundaryingest_ui(ctx):
    OUTPUT_DIR = ctx['OUTPUT_DIR']
    UPLOADS_DIR = ctx['UPLOADS_DIR']
    ERROR_DIR = ctx['ERROR_DIR']
    config_state = ctx['config_state']
    default_tenant = config_state.get('tenant_id', 'bi') or 'bi'

    api_url_input = widgets.Text(
        value='http://hcm-moz-impl.egov:8080/hcm-moz-impl/v1/dhis2/OU/ingest?source=EXCEL',
        description='API URL:',
        style={'description_width': '80px'},
        layout=widgets.Layout(width='600px'),
    )

    tenant_input = widgets.Text(
        value=default_tenant,
        description='Tenant ID:',
        placeholder='e.g. bi',
        style={'description_width': '120px'},
        layout=widgets.Layout(width='300px'),
    )

    project_type_input = widgets.Text(
        value='',
        description='Project Type ID:',
        placeholder='from Step 5',
        style={'description_width': '120px'},
        layout=widgets.Layout(width='500px'),
    )

    file_dropdown = widgets.Dropdown(
        options=[('Click Refresh to scan files', '')],
        description='CSV File:',
        style={'description_width': '80px'},
        layout=widgets.Layout(width='600px'),
    )

    refresh_btn = widgets.Button(description='Refresh', icon='refresh')
    ingest_btn = widgets.Button(description='Ingest', button_style='info', icon='upload')
    output_area = widgets.Output()
    verify_output = widgets.Output()

    def scan_boundary_csvs():
        """Scan output/csv_export_*/ for boundary CSV files."""
        patterns = [
            os.path.join(UPLOADS_DIR, '*.csv'),
            os.path.join(UPLOADS_DIR, '*.xls'),
            os.path.join(UPLOADS_DIR, '*.xlsx'),
        ]
        files = []
        for p in patterns:
            files.extend(glob.glob(p))
        return sorted(files)

    def on_refresh(btn):
        files = scan_boundary_csvs()
        if files:
            file_dropdown.options = [('Select a file', '')] + [
                (os.path.basename(f), f) for f in files
            ]
        else:
            file_dropdown.options = [('No boundary CSV files found', '')]
        file_dropdown.value = ''

    def on_ingest(btn):
        output_area.clear_output()
        with output_area:
            url = api_url_input.value.strip()
            tenant = tenant_input.value.strip()
            project_type = project_type_input.value.strip()
            csv_path = file_dropdown.value

            if not url:
                display(HTML('<p style="color:red">Please enter an API URL.</p>'))
                return
            if not tenant:
                display(HTML('<p style="color:red">Please enter a Tenant ID.</p>'))
                return
            if not project_type:
                display(HTML('<p style="color:red">Please enter a Project Type ID.</p>'))
                return
            if not csv_path:
                display(HTML('<p style="color:red">Please select a boundary CSV file.</p>'))
                return
            if not os.path.isfile(csv_path):
                display(HTML(f'<p style="color:red">File not found: {csv_path}</p>'))
                return

            display(HTML(f'<p>Ingesting <b>{os.path.basename(csv_path)}</b>...</p>'))
            try:
                result = ingest_boundary(url, tenant, project_type, csv_path)
                if result['success']:
                    display(HTML(
                        f'<p style="color:green"><b>Success</b> (HTTP {result["status_code"]})</p>'
                        f'<pre>{result["response"][:2000]}</pre>'
                    ))
                else:
                    display(HTML(
                        f'<p style="color:red"><b>Failed</b> (HTTP {result["status_code"]})</p>'
                        f'<pre>{result["response"][:2000]}</pre>'
                    ))
            except Exception as e:
                display(HTML(f'<p style="color:red"><b>Error:</b> {e}</p>'))

    refresh_btn.on_click(on_refresh)
    ingest_btn.on_click(on_ingest)

    # ====== Section B: Verify Ingestion from Boundary Search ======
    search_url_input = widgets.Text(
        value='http://boundary-service.egov:8080/boundary-service/boundary/_search',
        description='Search URL:',
        style={'description_width': '80px'},
        layout=widgets.Layout(width='600px'),
    )

    report_dir = os.path.join(OUTPUT_DIR, 'boundary_reports')
    verify_btn = widgets.Button(
        description='Generate Boundary Summary',
        button_style='success',
        icon='check',
    )

    progress_bar = widgets.IntProgress(
        value=0, min=0, max=1,
        description='Progress:',
        bar_style='info',
        style={'description_width': '80px'},
        layout=widgets.Layout(width='500px', visibility='hidden'),
    )
    progress_label = widgets.HTML(value='')
    progress_box = widgets.HBox([progress_bar, progress_label])

    def on_verify(btn):
        verify_output.clear_output()
        progress_bar.layout.visibility = 'hidden'
        progress_label.value = ''
        with verify_output:
            search_url = search_url_input.value.strip()
            tenant = tenant_input.value.strip() or default_tenant
            csv_path = file_dropdown.value

            if not search_url:
                display(HTML('<p style="color:red">Please enter the boundary search URL.</p>'))
                return
            if not csv_path:
                display(HTML('<p style="color:red">Please select the boundary CSV used for ingestion.</p>'))
                return
            if not os.path.isfile(csv_path):
                display(HTML(f'<p style="color:red">File not found: {csv_path}</p>'))
                return

            try:
                code_col, unique_codes = read_boundary_codes_from_csv(csv_path)
                if not unique_codes:
                    display(HTML(f'<p style="color:red">No codes found in column "{code_col}".</p>'))
                    return

                total = len(unique_codes)
                progress_bar.max = total
                progress_bar.value = 0
                progress_bar.layout.visibility = 'visible'
                progress_label.value = f'<span style="margin-left:10px">Checking 0/{total} codes...</span>'

                def on_progress(current, total, code, found):
                    progress_bar.value = current
                    status = '&#10003;' if found else '&#10007;'
                    progress_label.value = (
                        f'<span style="margin-left:10px">Checking {current}/{total} '
                        f'&mdash; <code>{code}</code> {status}</span>'
                    )

                result = verify_boundary_codes(
                    search_url, tenant_id=tenant, codes=unique_codes, progress_cb=on_progress,
                )

                progress_bar.bar_style = 'success'
                progress_label.value = (
                    f'<span style="margin-left:10px; color:#2e7d32; font-weight:bold">'
                    f'Done &mdash; {total} codes checked</span>'
                )

                found_codes = result['found_codes']
                not_found_codes = result['not_found_codes']
                found_count = len(found_codes)
                not_found_count = len(not_found_codes)
                rate = (found_count / total * 100) if total else 0

                summary = generate_boundary_ingestion_summary(
                    csv_path=csv_path,
                    found_codes=found_codes,
                    output_dir=report_dir,
                    code_column=code_col,
                )

                with open(summary['output_path'], 'rb') as dl:
                    b64 = base64.b64encode(dl.read()).decode()
                output_name = os.path.basename(summary['output_path'])

                rate_color = '#2e7d32' if rate >= 90 else '#e65100' if rate >= 50 else '#c62828'

                display(HTML(
                    '<div style="padding:15px; background:#f5f5f5; border-radius:8px; '
                    'margin-top:10px; border-left:4px solid #1976d2;">'
                    '<h4 style="margin:0 0 10px 0">Boundary Verification Summary</h4>'
                    '<table style="border-collapse:collapse; width:100%; max-width:400px">'
                    '<tr><td style="padding:4px 12px 4px 0">Total codes in CSV</td>'
                    f'<td style="padding:4px 0; font-weight:bold">{total}</td></tr>'
                    '<tr><td style="padding:4px 12px 4px 0">Found in system</td>'
                    f'<td style="padding:4px 0; font-weight:bold; color:#2e7d32">{found_count}</td></tr>'
                    '<tr><td style="padding:4px 12px 4px 0">Not found in system</td>'
                    f'<td style="padding:4px 0; font-weight:bold; color:#c62828">{not_found_count}</td></tr>'
                    '<tr><td style="padding:4px 12px 4px 0">Success rate</td>'
                    f'<td style="padding:4px 0; font-weight:bold; color:{rate_color}">{rate:.1f}%</td></tr>'
                    '<tr><td style="padding:4px 12px 4px 0">Code column</td>'
                    f'<td style="padding:4px 0"><code>{summary["code_column"]}</code></td></tr>'
                    '</table>'
                    f'<p style="margin:12px 0 0 0"><a href="data:application/vnd.openxmlformats-officedocument'
                    f'.spreadsheetml.sheet;base64,{b64}" '
                    f'download="{output_name}" style="display:inline-block; padding:8px 15px; background:#1976d2; '
                    f'color:white; text-decoration:none; border-radius:4px;">'
                    f'&#x2B07; Download {output_name}</a></p>'
                    '</div>'
                ))
            except Exception as e:
                progress_bar.bar_style = 'danger'
                progress_label.value = '<span style="margin-left:10px; color:#c62828">Failed</span>'
                display(HTML(f'<p style="color:red"><b>Error:</b> {e}</p>'))

    verify_btn.on_click(on_verify)

    return widgets.VBox([
        widgets.HTML('<h4>Boundary Ingestion</h4>'),
        widgets.HTML(
            '<div style="padding:10px; background:#f0f4ff; border-radius:5px; margin-bottom:10px">'
            '<b>Flow:</b> A) Ingest boundary CSV to server, B) verify with boundary search and generate Excel summary.'
            '</div>'
        ),
        widgets.HTML('<h4>A. Upload Boundary CSV</h4>'),
        api_url_input,
        tenant_input,
        project_type_input,
        widgets.HBox([file_dropdown, refresh_btn]),
        ingest_btn,
        output_area,
        widgets.HTML('<hr/>'),
        widgets.HTML('<h4>B. Verify Ingestion + Download Summary</h4>'),
        search_url_input,
        verify_btn,
        progress_box,
        verify_output,
    ])
