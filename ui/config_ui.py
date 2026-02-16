import ipywidgets as widgets
from IPython.display import display, HTML, clear_output


def build_config_ui(ctx):
    config_state = ctx['config_state']
    validator = ctx['validator']

    level_boxes = []
    target_boxes = []
    out_config = widgets.Output()

    # Boundary config widgets
    first_level = widgets.Text(
        value='', placeholder='e.g., COUNTRY', description='Level 1:',
        style={'description_width': '80px'}, layout=widgets.Layout(width='400px'))
    level_boxes.append(first_level)
    level_container = widgets.VBox([first_level])

    btn_add_level = widgets.Button(
        description='+ Add Level', button_style='info',
        layout=widgets.Layout(width='120px'))

    def add_level(btn):
        n = len(level_boxes) + 1
        box = widgets.Text(
            value='', placeholder='e.g., District', description=f'Level {n}:',
            style={'description_width': '80px'}, layout=widgets.Layout(width='400px'))
        level_boxes.append(box)
        level_container.children = list(level_boxes)

    btn_add_level.on_click(add_level)

    num_targets = widgets.IntText(
        value=0, description='# Targets:', style={'description_width': '80px'},
        layout=widgets.Layout(width='200px'))
    target_container = widgets.VBox([])

    def update_targets(change):
        target_boxes.clear()
        for i in range(change['new']):
            box = widgets.Text(
                value=f'target_{i+1}', description=f'Target {i+1}:',
                style={'description_width': '80px'}, layout=widgets.Layout(width='400px'))
            target_boxes.append(box)
        target_container.children = target_boxes

    num_targets.observe(update_targets, names='value')

    # Facility config with mapping
    facility_col = widgets.Text(
        value='Facility Name', description='Facility:',
        style={'description_width': '100px'}, layout=widgets.Layout(width='280px'))
    facility_map = widgets.Text(
        value='Unidade Sanitaria', description='Maps to:',
        style={'description_width': '70px'}, layout=widgets.Layout(width='220px'))

    district_col = widgets.Text(
        value='District', description='District:',
        style={'description_width': '100px'}, layout=widgets.Layout(width='280px'))
    district_map = widgets.Text(
        value='Distrito', description='Maps to:',
        style={'description_width': '70px'}, layout=widgets.Layout(width='220px'))

    state_col = widgets.Text(
        value='State', description='State:',
        style={'description_width': '100px'}, layout=widgets.Layout(width='280px'))
    state_map = widgets.Text(
        value='Provincia', description='Maps to:',
        style={'description_width': '70px'}, layout=widgets.Layout(width='220px'))

    btn_save = widgets.Button(
        description='Save Config', button_style='success',
        layout=widgets.Layout(width='150px'))

    def save_config(btn):
        with out_config:
            clear_output(wait=True)
            levels = [b.value.strip() for b in level_boxes if b.value.strip()]
            targets = [b.value.strip() for b in target_boxes if b.value.strip()]

            if not levels:
                display(HTML("<p style='color:red'>Enter at least one level!</p>"))
                return

            mapping = {}
            if facility_col.value.strip() and facility_map.value.strip():
                mapping[facility_col.value.strip()] = facility_map.value.strip()
            if district_col.value.strip() and district_map.value.strip():
                mapping[district_col.value.strip()] = district_map.value.strip()
            if state_col.value.strip() and state_map.value.strip():
                mapping[state_col.value.strip()] = state_map.value.strip()

            config_state.update({
                'level_columns': levels, 'target_columns': targets, 'num_targets': len(targets),
                'facility_col': facility_col.value.strip(), 'district_col': district_col.value.strip(),
                'state_col': state_col.value.strip(), 'alignment_mapping': mapping, 'configured': True
            })

            validator.set_columns(
                boundary_cols=levels,
                facility_cols=[facility_col.value.strip()] if facility_col.value.strip() else [],
                target_cols=targets, num_targets=len(targets))

            map_str = ', '.join([f'{k}\u2192{v}' for k, v in mapping.items()])
            display(HTML(f"""<div style='padding:10px; background:#d4edda; border-radius:5px;'>
                <b>Saved!</b><br>Levels: {', '.join(levels)}<br>Targets: {', '.join(targets) or 'none'}
                <br>Mapping: {map_str or 'none'}</div>"""))

    btn_save.on_click(save_config)

    return widgets.VBox([
        widgets.HTML("<h3>Boundary File</h3>"),
        level_container, btn_add_level,
        widgets.HTML("<b>Targets:</b>"), num_targets, target_container,
        widgets.HTML("<h3>Facility File (with mapping to boundary)</h3>"),
        widgets.HBox([facility_col, facility_map]),
        widgets.HBox([district_col, district_map]),
        widgets.HBox([state_col, state_map]),
        btn_save, out_config
    ])
