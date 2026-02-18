import os
import ipywidgets as widgets
from IPython.display import display, HTML, clear_output
from utils.common import detect_columns


def build_config_ui(ctx):
    config_state = ctx['config_state']
    file_state = ctx['file_state']
    validator = ctx['validator']
    UPLOADS_DIR = ctx['UPLOADS_DIR']

    level_boxes = []
    target_boxes = []
    boundary_headers = []
    facility_headers = []
    out_config = widgets.Output()
    out_detect = widgets.Output()

    # ── Section A: File Upload ──
    upload_boundary = widgets.FileUpload(
        accept='.xlsx,.xls,.csv', multiple=False, description='Boundary File')
    upload_facility = widgets.FileUpload(
        accept='.xlsx,.xls,.csv', multiple=False, description='Facility File')
    w_header_row = widgets.IntText(
        value=1, description='Header Row:', style={'description_width': '90px'},
        layout=widgets.Layout(width='200px'))

    btn_detect = widgets.Button(
        description='Detect Headers', button_style='info',
        layout=widgets.Layout(width='150px'))

    # ── Section B: Boundary Config (dropdowns) ──
    PLACEHOLDER = [('(upload file first)', '')]

    first_level = widgets.Dropdown(
        options=PLACEHOLDER, description='Level 1:',
        style={'description_width': '80px'}, layout=widgets.Layout(width='400px'))
    level_boxes.append(first_level)
    level_container = widgets.VBox([first_level])

    btn_add_level = widgets.Button(
        description='+ Add Level', button_style='info',
        layout=widgets.Layout(width='120px'))

    num_targets = widgets.IntText(
        value=0, description='# Targets:', style={'description_width': '80px'},
        layout=widgets.Layout(width='200px'))
    target_container = widgets.VBox([])

    # ── Section C: Facility Config (dropdowns) ──
    facility_col = widgets.Dropdown(
        options=PLACEHOLDER, description='Facility:',
        style={'description_width': '100px'}, layout=widgets.Layout(width='300px'))
    facility_map = widgets.Dropdown(
        options=PLACEHOLDER, description='Maps to:',
        style={'description_width': '70px'}, layout=widgets.Layout(width='250px'))

    district_col = widgets.Dropdown(
        options=PLACEHOLDER, description='District:',
        style={'description_width': '100px'}, layout=widgets.Layout(width='300px'))
    district_map = widgets.Dropdown(
        options=PLACEHOLDER, description='Maps to:',
        style={'description_width': '70px'}, layout=widgets.Layout(width='250px'))

    state_col = widgets.Dropdown(
        options=PLACEHOLDER, description='State:',
        style={'description_width': '100px'}, layout=widgets.Layout(width='300px'))
    state_map = widgets.Dropdown(
        options=PLACEHOLDER, description='Maps to:',
        style={'description_width': '70px'}, layout=widgets.Layout(width='250px'))

    def _save_upload(uploader, key):
        """Save uploaded file to UPLOADS_DIR and store path in file_state."""
        if not uploader.value:
            return None
        files = uploader.value
        info = files[0] if isinstance(files, tuple) else list(files.values())[0]
        name = info.name if hasattr(info, 'name') else info['name']
        content = info.content if hasattr(info, 'content') else info['content']
        path = os.path.join(UPLOADS_DIR, name)
        with open(path, 'wb') as f:
            f.write(content)
        file_state[key] = path
        return path

    def _make_options(headers):
        """Build dropdown options list from header names."""
        if not headers:
            return PLACEHOLDER
        return [('-- select --', '')] + [(h, h) for h in headers]

    def _refresh_boundary_dropdowns():
        """Update all boundary-sourced dropdowns with current boundary_headers."""
        opts = _make_options(boundary_headers)
        for box in level_boxes:
            old = box.value
            box.options = opts
            if old in boundary_headers:
                box.value = old
        # Refresh target dropdowns
        for box in target_boxes:
            old = box.value
            box.options = opts
            if old in boundary_headers:
                box.value = old
        # Refresh facility "maps to" dropdowns
        for dd in [facility_map, district_map, state_map]:
            old = dd.value
            dd.options = opts
            if old in boundary_headers:
                dd.value = old

    def _refresh_facility_dropdowns():
        """Update all facility-sourced dropdowns with current facility_headers."""
        opts = _make_options(facility_headers)
        for dd in [facility_col, district_col, state_col]:
            old = dd.value
            dd.options = opts
            if old in facility_headers:
                dd.value = old

    def on_detect(btn):
        with out_detect:
            clear_output(wait=True)

            # Save files if uploaded
            _save_upload(upload_boundary, 'boundary_file')
            _save_upload(upload_facility, 'facility_file')

            b_path = file_state.get('boundary_file')
            f_path = file_state.get('facility_file')

            if not b_path and not f_path:
                display(HTML("<p style='color:red'>Upload at least one file to detect headers.</p>"))
                return

            hr = w_header_row.value
            msgs = []

            boundary_headers.clear()
            facility_headers.clear()

            if b_path and os.path.exists(b_path):
                try:
                    b_map = detect_columns(b_path, hr)
                    boundary_headers.extend(b_map.keys())
                    msgs.append(f"Boundary: {len(boundary_headers)} headers detected")
                except Exception as e:
                    msgs.append(f"<span style='color:red'>Boundary error: {e}</span>")

            if f_path and os.path.exists(f_path):
                try:
                    f_map = detect_columns(f_path, hr)
                    facility_headers.extend(f_map.keys())
                    msgs.append(f"Facility: {len(facility_headers)} headers detected")
                except Exception as e:
                    msgs.append(f"<span style='color:red'>Facility error: {e}</span>")

            _refresh_boundary_dropdowns()
            _refresh_facility_dropdowns()

            display(HTML(
                f"<div style='padding:8px; background:#d4edda; border-radius:4px;'>"
                f"{'<br>'.join(msgs)}</div>"))

    btn_detect.on_click(on_detect)

    def add_level(btn):
        n = len(level_boxes) + 1
        box = widgets.Dropdown(
            options=_make_options(boundary_headers), description=f'Level {n}:',
            style={'description_width': '80px'}, layout=widgets.Layout(width='400px'))
        level_boxes.append(box)
        level_container.children = list(level_boxes)

    btn_add_level.on_click(add_level)

    def update_targets(change):
        target_boxes.clear()
        opts = _make_options(boundary_headers)
        for i in range(change['new']):
            box = widgets.Dropdown(
                options=opts, description=f'Target {i+1}:',
                style={'description_width': '80px'}, layout=widgets.Layout(width='400px'))
            target_boxes.append(box)
        target_container.children = target_boxes

    num_targets.observe(update_targets, names='value')

    # ── Save button ──
    btn_save = widgets.Button(
        description='Save Config', button_style='success',
        layout=widgets.Layout(width='150px'))

    def save_config(btn):
        with out_config:
            clear_output(wait=True)
            levels = [b.value.strip() for b in level_boxes if b.value and b.value.strip()]
            targets = [b.value.strip() for b in target_boxes if b.value and b.value.strip()]

            if not levels:
                display(HTML("<p style='color:red'>Select at least one boundary level!</p>"))
                return

            mapping = {}
            if facility_col.value and facility_map.value:
                mapping[facility_col.value] = facility_map.value
            if district_col.value and district_map.value:
                mapping[district_col.value] = district_map.value
            if state_col.value and state_map.value:
                mapping[state_col.value] = state_map.value

            config_state.update({
                'level_columns': levels, 'target_columns': targets, 'num_targets': len(targets),
                'facility_col': facility_col.value or '', 'district_col': district_col.value or '',
                'state_col': state_col.value or '', 'alignment_mapping': mapping, 'configured': True
            })

            validator.set_columns(
                boundary_cols=levels,
                facility_cols=[facility_col.value] if facility_col.value else [],
                target_cols=targets, num_targets=len(targets))

            map_str = ', '.join([f'{k}\u2192{v}' for k, v in mapping.items()])
            display(HTML(f"""<div style='padding:10px; background:#d4edda; border-radius:5px;'>
                <b>Saved!</b><br>Levels: {', '.join(levels)}<br>Targets: {', '.join(targets) or 'none'}
                <br>Mapping: {map_str or 'none'}</div>"""))

    btn_save.on_click(save_config)

    return widgets.VBox([
        widgets.HTML("<h3>Step 1: Upload Files</h3>"),
        widgets.HBox([
            widgets.VBox([widgets.HTML('<b>Boundary File:</b>'), upload_boundary]),
            widgets.VBox([widgets.HTML('<b>Facility File:</b>'), upload_facility]),
        ]),
        widgets.HBox([w_header_row, btn_detect]),
        out_detect,
        widgets.HTML("<h3>Step 2: Boundary Levels (from boundary file headers)</h3>"),
        level_container, btn_add_level,
        widgets.HTML("<b>Targets:</b>"), num_targets, target_container,
        widgets.HTML("<h3>Step 3: Facility Mapping</h3>"),
        widgets.HTML("<em>Left = facility file column, Right = boundary file column it maps to</em>"),
        widgets.HBox([facility_col, facility_map]),
        widgets.HBox([district_col, district_map]),
        widgets.HBox([state_col, state_map]),
        btn_save, out_config
    ])
