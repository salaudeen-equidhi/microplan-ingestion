import csv
import os


# Columns that new facility rows need — ensured in fieldnames
REQUIRED_COLUMNS = (
    'facility_name', 'facility_type', 'boundary_code',
    'administrative_area', 'parent_code', 'target', 'is_permanent',
)


def build_facility_excel(facility_csv_path, boundary_csv_path, output_path,
                         selected_levels, facility_type_names, level_order):
    """
    Build a facility CSV with higher-level facility rows added.

    Args:
        facility_csv_path: Path to the exported facility CSV
        boundary_csv_path: Path to the exported boundary CSV
        output_path: Where to write the built CSV
        selected_levels: List of boundary_type names to add facilities for
                         e.g. ["Distrito", "Provincia"]
        facility_type_names: Dict mapping boundary_type name to facility_type string
                             e.g. {"Distrito": "District Facility", "Provincia": "State Facility"}
        level_order: Full list of level names from config (index 0 = Country, 1 = State, etc.)
                     Used to determine which level is higher/lower.

    Parent code logic:
        - Existing (Health Facility) rows: parent_code is CLEARED
        - Highest selected level (e.g. State): parent_code = comma-separated
          boundary_codes of all next-lower-level boundaries under it
        - Lower selected levels (e.g. District): parent_code = boundary_code of
          their parent at the next-higher selected level

    Returns:
        dict with counts and warnings
    """
    warnings = []

    # --- Read ALL boundaries, skip empty codes ---
    all_boundaries = []
    with open(boundary_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get('code', '').strip()
            if not code:
                continue  # skip boundaries with empty codes
            all_boundaries.append({
                'name': row.get('name', '').strip(),
                'code': code,
                'boundary_type': row.get('boundary_type', '').strip(),
                'parent_code': row.get('parent_code', '').strip(),
            })

    # Build code -> boundary lookup
    code_to_boundary = {b['code']: b for b in all_boundaries}

    # Group boundaries by type, deduplicate by code
    boundaries_by_type = {}
    seen_codes = set()
    for b in all_boundaries:
        if b['code'] not in seen_codes:
            seen_codes.add(b['code'])
            boundaries_by_type.setdefault(b['boundary_type'], []).append(b)

    # Sort selected levels by their position in level_order (highest first = lowest index)
    def level_index(name):
        try:
            return level_order.index(name)
        except ValueError:
            return 999

    sorted_selected = sorted(selected_levels, key=level_index)

    # --- Build parent-child mapping between selected levels ---
    parent_level_map = {}  # level_name -> higher selected level_name
    for i in range(1, len(sorted_selected)):
        parent_level_map[sorted_selected[i]] = sorted_selected[i - 1]

    child_to_parent = {}   # (lower_level, lower_code) -> higher_code
    parent_to_children = {}  # (higher_level, higher_code) -> [lower_codes]

    higher_level_codes = {}
    for level_name in sorted_selected:
        higher_level_codes[level_name] = {
            b['code'] for b in boundaries_by_type.get(level_name, [])
        }

    for lower_level, higher_level in parent_level_map.items():
        target_codes = higher_level_codes[higher_level]
        for b in boundaries_by_type.get(lower_level, []):
            current = b['code']
            found = None
            for _ in range(20):
                parent_code = code_to_boundary.get(current, {}).get('parent_code', '')
                if not parent_code:
                    break
                if parent_code in target_codes:
                    found = parent_code
                    break
                current = parent_code

            if found:
                child_to_parent[(lower_level, b['code'])] = found
                parent_to_children.setdefault((higher_level, found), []).append(b['code'])
            else:
                warnings.append(
                    f"Could not find {higher_level} ancestor for "
                    f"{lower_level} boundary '{b['name']}' ({b['code']})"
                )

    # --- Read existing facility rows ---
    with open(facility_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    # Ensure all required columns exist in fieldnames
    for col in REQUIRED_COLUMNS:
        if col not in fieldnames:
            fieldnames.append(col)

    # --- Detect re-build: check if rows already contain builder-added facility types ---
    builder_types = set(facility_type_names.values())
    existing_types = {row.get('facility_type', '').strip() for row in rows}
    duplicate_types = builder_types & existing_types
    if duplicate_types:
        # Remove previously-added builder rows to prevent duplicates
        original_len = len(rows)
        rows = [r for r in rows if r.get('facility_type', '').strip() not in builder_types]
        removed = original_len - len(rows)
        if removed:
            warnings.append(
                f"Removed {removed} previously-built rows "
                f"(types: {', '.join(duplicate_types)}) to prevent duplicates."
            )

    # --- Process existing rows ---
    admin_filled = 0
    for row in rows:
        # Copy facility_name -> administrative_area
        fname = row.get('facility_name', '').strip()
        if fname:
            row['administrative_area'] = fname
            admin_filled += 1
        # Clear parent_code for existing (health facility) rows
        row['parent_code'] = ''

    original_count = len(rows)

    # --- Add new facility rows (lower levels first, highest last) ---
    added = 0
    for level_name in reversed(sorted_selected):
        ftype = facility_type_names.get(level_name, f'{level_name} Facility')
        higher_level = parent_level_map.get(level_name)

        for boundary in boundaries_by_type.get(level_name, []):
            new_row = {fn: '' for fn in fieldnames}
            new_row['facility_name'] = boundary['name']
            new_row['facility_type'] = ftype
            new_row['boundary_code'] = boundary['code']
            new_row['administrative_area'] = boundary['name']
            new_row['target'] = '0'
            new_row['is_permanent'] = 'TRUE'

            if higher_level:
                # Lower level (e.g. District) -> parent_code = higher level boundary code
                ancestor = child_to_parent.get((level_name, boundary['code']), '')
                new_row['parent_code'] = ancestor
            else:
                # Highest selected level (e.g. State)
                # parent_code = comma-separated codes of next-lower selected level
                children = parent_to_children.get((level_name, boundary['code']), [])
                new_row['parent_code'] = ','.join(children) if children else ''

            rows.append(new_row)
            added += 1

    # --- Write output ---
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return {
        'original': original_count,
        'added': added,
        'admin_filled': admin_filled,
        'total': original_count + added,
        'warnings': warnings,
    }
