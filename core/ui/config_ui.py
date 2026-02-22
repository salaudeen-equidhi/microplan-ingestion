import os
import glob
import ipywidgets as widgets
from IPython.display import display, HTML, clear_output
from utils.common import detect_columns, classify_columns, CASING_OPTIONS, set_casing_mode


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

    # ── Section A: Select Files (dropdown + manual path) ──
    style_wide = {'description_width': '130px'}

    boundary_dropdown = widgets.Dropdown(
        options=[('Click Refresh to scan', '')],
        description='Boundary File:',
        style=style_wide, layout=widgets.Layout(width='600px'))

    facility_dropdown = widgets.Dropdown(
        options=[('Click Refresh to scan', '')],
        description='Facility File:',
        style=style_wide, layout=widgets.Layout(width='600px'))

    boundary_manual = widgets.Text(
        value=file_state.get('boundary_file') or '',
        placeholder='or type boundary file path here',
        description='',
        layout=widgets.Layout(width='600px'))

    facility_manual = widgets.Text(
        value=file_state.get('facility_file') or '',
        placeholder='or type facility file path here',
        description='',
        layout=widgets.Layout(width='600px'))

    def scan_files():
        files = glob.glob(os.path.join(UPLOADS_DIR, '*.xlsx'))
        return sorted(files)

    btn_refresh = widgets.Button(
        description='Refresh', button_style='info', icon='refresh',
        layout=widgets.Layout(width='100px'))

    def on_refresh(btn):
        files = scan_files()
        if files:
            opts = [('-- select --', '')] + [(os.path.basename(f), f) for f in files]
        else:
            opts = [('No .xlsx files found', '')]
        boundary_dropdown.options = opts
        facility_dropdown.options = opts
        boundary_dropdown.value = ''
        facility_dropdown.value = ''

    btn_refresh.on_click(on_refresh)

    w_header_row = widgets.IntText(
        value=1, description='Header Row:', style={'description_width': '90px'},
        layout=widgets.Layout(width='200px'))

    btn_detect = widgets.Button(
        description='Detect Headers', button_style='info',
        layout=widgets.Layout(width='150px'))

    # ── Section B: Boundary Config (dropdowns) ──
    PLACEHOLDER = [('(detect headers first)', '')]

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

    def _get_boundary_path():
        return boundary_manual.value.strip() or boundary_dropdown.value or ''

    def _get_facility_path():
        return facility_manual.value.strip() or facility_dropdown.value or ''

    def _make_options(headers):
        if not headers:
            return PLACEHOLDER
        return [('-- select --', '')] + [(h, h) for h in headers]

    def _refresh_boundary_dropdowns():
        opts = _make_options(boundary_headers)
        for box in level_boxes:
            old = box.value
            box.options = opts
            if old in boundary_headers:
                box.value = old
        for box in target_boxes:
            old = box.value
            box.options = opts
            if old in boundary_headers:
                box.value = old
        for dd in [facility_map, district_map, state_map]:
            old = dd.value
            dd.options = opts
            if old in boundary_headers:
                dd.value = old

    def _refresh_facility_dropdowns():
        opts = _make_options(facility_headers)
        for dd in [facility_col, district_col, state_col]:
            old = dd.value
            dd.options = opts
            if old in facility_headers:
                dd.value = old

    def _auto_populate_levels_and_targets(detected_levels, detected_targets):
        """Auto-create level and target dropdowns with pre-selected values."""
        opts = _make_options(boundary_headers)

        # Rebuild level dropdowns
        level_boxes.clear()
        for i, col_name in enumerate(detected_levels):
            box = widgets.Dropdown(
                options=opts, value=col_name, description=f'Level {i+1}:',
                style={'description_width': '80px'}, layout=widgets.Layout(width='400px'))
            level_boxes.append(box)
        level_container.children = list(level_boxes)

        # Set num_targets — this fires update_targets observer
        num_targets.value = len(detected_targets)

        # After observer creates target_boxes, set their values
        for i, col_name in enumerate(detected_targets):
            if i < len(target_boxes):
                target_boxes[i].value = col_name

    def on_detect(btn):
        with out_detect:
            clear_output(wait=True)

            b_path = _get_boundary_path()
            f_path = _get_facility_path()

            # Store paths in file_state
            if b_path:
                file_state['boundary_file'] = b_path
            if f_path:
                file_state['facility_file'] = f_path

            if not b_path and not f_path:
                display(HTML("<p style='color:red'>Select or enter at least one file path first.</p>"))
                return

            hr = w_header_row.value
            msgs = []

            boundary_headers.clear()
            facility_headers.clear()
            detected_levels = []
            detected_targets = []

            if b_path and os.path.exists(b_path):
                try:
                    b_map = detect_columns(b_path, hr)
                    boundary_headers.extend(b_map.keys())
                    detected_levels, detected_targets = classify_columns(b_path, hr)
                    msgs.append(
                        f"Boundary: {len(boundary_headers)} headers "
                        f"({len(detected_levels)} levels, {len(detected_targets)} targets)")
                except Exception as e:
                    msgs.append(f"<span style='color:red'>Boundary error: {e}</span>")
            elif b_path:
                msgs.append(f"<span style='color:red'>Boundary file not found: {b_path}</span>")

            if f_path and os.path.exists(f_path):
                try:
                    f_map = detect_columns(f_path, hr)
                    facility_headers.extend(f_map.keys())
                    msgs.append(f"Facility: {len(facility_headers)} headers detected")
                except Exception as e:
                    msgs.append(f"<span style='color:red'>Facility error: {e}</span>")
            elif f_path:
                msgs.append(f"<span style='color:red'>Facility file not found: {f_path}</span>")

            _refresh_boundary_dropdowns()
            _refresh_facility_dropdowns()

            # Auto-populate levels and targets if detected
            if detected_levels or detected_targets:
                _auto_populate_levels_and_targets(detected_levels, detected_targets)

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

    # ── Section D: Name Casing ──
    casing_dropdown = widgets.Dropdown(
        options=[(label, key) for key, label in CASING_OPTIONS.items()],
        value='none',
        description='Name Casing:',
        style={'description_width': '100px'},
        layout=widgets.Layout(width='400px'))

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

            # Update file_state from current selections
            b_path = _get_boundary_path()
            f_path = _get_facility_path()
            if b_path:
                file_state['boundary_file'] = b_path
            if f_path:
                file_state['facility_file'] = f_path

            mapping = {}
            if facility_col.value and facility_map.value:
                mapping[facility_col.value] = facility_map.value
            if district_col.value and district_map.value:
                mapping[district_col.value] = district_map.value
            if state_col.value and state_map.value:
                mapping[state_col.value] = state_map.value

            set_casing_mode(casing_dropdown.value)

            config_state.update({
                'level_columns': levels, 'target_columns': targets, 'num_targets': len(targets),
                'facility_col': facility_col.value or '', 'district_col': district_col.value or '',
                'state_col': state_col.value or '', 'alignment_mapping': mapping,
                'casing_mode': casing_dropdown.value, 'configured': True
            })

            validator.set_columns(
                boundary_cols=levels,
                facility_cols=[facility_col.value] if facility_col.value else [],
                target_cols=targets, num_targets=len(targets))

            map_str = ', '.join([f'{k}\u2192{v}' for k, v in mapping.items()])
            casing_label = CASING_OPTIONS.get(casing_dropdown.value, 'none')
            display(HTML(f"""<div style='padding:10px; background:#d4edda; border-radius:5px;'>
                <b>Saved!</b><br>Levels: {', '.join(levels)}<br>Targets: {', '.join(targets) or 'none'}
                <br>Mapping: {map_str or 'none'}<br>Name casing: {casing_label}</div>"""))

    btn_save.on_click(save_config)

    return widgets.VBox([
        widgets.HTML("<h3>Step 1: Select Files</h3>"),
        widgets.HTML("<em>Select from dropdown (click Refresh first) or type the file path manually</em>"),
        widgets.HTML('<b>Boundary File:</b>'),
        widgets.HBox([boundary_dropdown, btn_refresh]),
        boundary_manual,
        widgets.HTML('<b>Facility File:</b>'),
        facility_dropdown,
        facility_manual,
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
        widgets.HTML("<h3>Step 4: Name Casing</h3>"),
        widgets.HTML("<em>Auto-normalize all boundary and facility names during transform</em>"),
        casing_dropdown,
        btn_save, out_config
    ])
