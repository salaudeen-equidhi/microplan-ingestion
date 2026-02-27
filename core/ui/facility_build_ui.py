import os
import glob
import base64
import ipywidgets as widgets
from IPython.display import display, HTML
from utils.facility_builder import build_facility_excel


def build_facility_build_ui(ctx):
    OUTPUT_DIR = ctx['OUTPUT_DIR']
    UPLOADS_DIR = ctx['UPLOADS_DIR']
    config_state = ctx['config_state']

    level_names = [col for col in config_state.get('level_columns', []) if col]

    # --- CSV selectors ---
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
        boundary_dropdown.options = opts
        facility_dropdown.options = opts
        boundary_dropdown.value = ''
        facility_dropdown.value = ''

    refresh_btn.on_click(on_refresh)

    # --- Level checkboxes + type inputs ---
    # Skip first level (Country) — user picks from level 2 onward
    selectable_levels = level_names[1:] if len(level_names) > 1 else level_names

    level_checkboxes = []
    type_inputs = []
    for name in selectable_levels:
        cb = widgets.Checkbox(
            value=False,
            description=name,
            indent=False,
            layout=widgets.Layout(width='200px'),
        )
        ti = widgets.Text(
            value=f'{name.capitalize()} Facility',
            description='Type:',
            style={'description_width': '50px'},
            layout=widgets.Layout(width='300px'),
        )
        level_checkboxes.append(cb)
        type_inputs.append(ti)

    level_rows = [widgets.HBox([cb, ti]) for cb, ti in zip(level_checkboxes, type_inputs)]

    # --- Build button ---
    build_btn = widgets.Button(
        description='Build Facility File',
        button_style='success',
        icon='wrench',
    )
    output_area = widgets.Output()

    def on_build(btn):
        output_area.clear_output()
        with output_area:
            boundary_path = boundary_manual.value.strip() or boundary_dropdown.value
            if not boundary_path or not os.path.isfile(boundary_path):
                display(HTML('<p style="color:red">Please select or enter the boundary CSV file path.</p>'))
                return

            facility_path = facility_manual.value.strip() or facility_dropdown.value
            if not facility_path or not os.path.isfile(facility_path):
                display(HTML('<p style="color:red">Please select or enter the facility CSV file path.</p>'))
                return

            selected_levels = []
            facility_type_names = {}
            for cb, ti, name in zip(level_checkboxes, type_inputs, selectable_levels):
                if cb.value:
                    selected_levels.append(name)
                    facility_type_names[name] = ti.value.strip() or f'{name} Facility'

            if not selected_levels:
                display(HTML('<p style="color:red">Please select at least one boundary level.</p>'))
                return

            display(HTML('<p>Building facility file...</p>'))

            try:
                out_dir = os.path.join(OUTPUT_DIR, 'facility_build')
                base_name = os.path.basename(facility_path).rsplit('.', 1)[0]
                output_name = f'{base_name}_built.csv'
                output_path = os.path.join(out_dir, output_name)

                result = build_facility_excel(
                    facility_csv_path=facility_path,
                    boundary_csv_path=boundary_path,
                    output_path=output_path,
                    selected_levels=selected_levels,
                    facility_type_names=facility_type_names,
                    level_order=level_names,
                )

                with open(output_path, 'rb') as dl:
                    b64 = base64.b64encode(dl.read()).decode()

                levels_str = ', '.join(selected_levels)
                warn_html = ''
                if result.get('warnings'):
                    warn_items = ''.join(
                        f'<li>{w}</li>' for w in result['warnings']
                    )
                    warn_html = (
                        f'<div style="padding:8px; background:#fff3cd; border-left:4px solid #ffc107; '
                        f'margin:8px 0; border-radius:3px;">'
                        f'<b style="color:#856404;">Warnings:</b><ul style="margin:4px 0">'
                        f'{warn_items}</ul></div>'
                    )

                display(HTML(
                    f'<div style="padding:12px; background:#e8f4fd; border-left:4px solid #4caf50; margin:10px 0;">'
                    f'<b style="color:#2e7d32;">Facility file built successfully!</b><br><br>'
                    f'<table style="border-collapse:collapse">'
                    f'<tr><td style="padding:3px 12px 3px 0">Original facility rows</td>'
                    f'<td style="font-weight:bold">{result["original"]}</td></tr>'
                    f'<tr><td style="padding:3px 12px 3px 0">Rows with admin area filled</td>'
                    f'<td style="font-weight:bold">{result["admin_filled"]}</td></tr>'
                    f'<tr><td style="padding:3px 12px 3px 0">New facility rows added</td>'
                    f'<td style="font-weight:bold">{result["added"]}</td></tr>'
                    f'<tr><td style="padding:3px 12px 3px 0">Total rows in output</td>'
                    f'<td style="font-weight:bold">{result["total"]}</td></tr>'
                    f'<tr><td style="padding:3px 12px 3px 0">Levels added</td>'
                    f'<td>{levels_str}</td></tr>'
                    f'</table>'
                    f'{warn_html}<br>'
                    f'<a href="data:text/csv;base64,{b64}" download="{output_name}" '
                    f'style="display:inline-block; padding:8px 15px; background:#4caf50; color:white; '
                    f'text-decoration:none; border-radius:4px;">'
                    f'Download {output_name}</a>'
                    f'</div>'
                ))
            except Exception as e:
                import traceback
                display(HTML(
                    f'<div style="padding:10px; background:#ffc7ce; border-radius:5px;">'
                    f'<b style="color:red">Error:</b><br><pre>{traceback.format_exc()}</pre></div>'
                ))

    build_btn.on_click(on_build)

    # --- Layout ---
    help_text = (
        '<div style="padding:10px; background:#f0f4ff; border-radius:5px; margin-bottom:10px">'
        '<b>Instructions:</b><br/>'
        '1. Select the <b>boundary CSV</b> and <b>facility CSV</b> exported in the previous step<br/>'
        '2. Check the boundary levels that should get their own facility rows '
        '(e.g. District, State)<br/>'
        '3. Optionally edit the facility type name for each level<br/>'
        '4. Click <b>Build Facility File</b><br/>'
        '5. The output CSV will have: original rows with <code>administrative_area</code> '
        'filled + new higher-level facility rows<br/>'
        '6. Use this built CSV in the next step (Facility Ingestion)'
        '</div>'
    )

    children = [
        widgets.HTML('<h3>Build Facility File</h3>'),
        widgets.HTML(help_text),
        widgets.HTML('<b>Boundary CSV</b>'),
        widgets.HBox([boundary_dropdown, refresh_btn]),
        boundary_manual,
        widgets.HTML('<b>Facility CSV</b>'),
        facility_dropdown,
        facility_manual,
        widgets.HTML('<b>Select boundary levels to add as facilities:</b>'),
    ]
    children.extend(level_rows)
    children.extend([
        build_btn,
        output_area,
    ])

    return widgets.VBox(children)
