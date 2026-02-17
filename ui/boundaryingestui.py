import os
import glob
import ipywidgets as widgets
from IPython.display import display, HTML
from utils.api import ingest_boundary


def build_boundaryingest_ui(ctx):
    OUTPUT_DIR = ctx['OUTPUT_DIR']

    api_url_input = widgets.Text(
        value='http://hcm-moz-impl.egov:8080/hcm-moz-impl/v1/dhis2/OU/ingest?source=EXCEL',
        description='API URL:',
        style={'description_width': '80px'},
        layout=widgets.Layout(width='600px'),
    )

    tenant_input = widgets.Text(
        value='',
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

    def scan_boundary_csvs():
        """Scan output/csv_export_*/ for boundary CSV files."""
        pattern = os.path.join(OUTPUT_DIR, 'csv_export_*', '*boundar*.csv')
        return sorted(glob.glob(pattern))

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

    return widgets.VBox([
        widgets.HTML('<h4>Boundary Ingestion</h4>'),
        api_url_input,
        tenant_input,
        project_type_input,
        widgets.HBox([file_dropdown, refresh_btn]),
        ingest_btn,
        output_area,
    ])
