import csv


def create_uuid_name_map(facility_csv_path, output_path):
    """
    Create a uuid_name_map.csv from the facility CSV.

    Reads the facility CSV, keeps only 'uuid' and 'facility_name' columns,
    renames 'facility_name' to 'name', and writes to output_path.

    Returns the number of rows written.
    """
    rows_written = 0
    with open(facility_csv_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=['uuid', 'name'])
        writer.writeheader()

        for row in reader:
            writer.writerow({
                'uuid': row.get('uuid', '').strip(),
                'name': row.get('facility_name', '').strip(),
            })
            rows_written += 1

    return rows_written


def replace_names_with_uuids(facility_csv_path, output_path):
    """
    Replace facility_name values with UUID values in the facility CSV.

    For Warehouse and Health Facility rows, the facility_name column
    gets replaced with the uuid value. This is expected by the ingestion API;
    correct names are restored later using the uuid_name_map.

    Returns the number of rows where names were replaced.
    """
    replaced = 0
    with open(facility_csv_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            facility_type = row.get('facility_type', '').strip()
            uuid_val = row.get('uuid', '').strip()
            if facility_type in ('Warehouse', 'Health Facility') and uuid_val:
                row['facility_name'] = uuid_val
                replaced += 1
            writer.writerow(row)

    return replaced


def generate_hf_district_mapping(boundary_csv_path, hf_parent_type, district_type):
    """
    Build a mapping from health-facility boundary codes to district codes.

    Reads the exported boundary CSV and walks up the parent chain from
    hf_parent_type boundaries until reaching a district_type boundary.
    Handles any number of intermediate levels between them.

    Returns dict  {hf_parent_boundary_code: district_code}
    """
    hf_parent_codes = []        # codes of the HF-parent-level boundaries
    district_code_set = set()   # codes that are district-level boundaries
    code_to_parent = {}         # all boundaries: code -> parent_code

    with open(boundary_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            btype = row['boundary_type'].strip()
            code = row['code'].strip()
            parent_code = row['parent_code'].strip()
            code_to_parent[code] = parent_code

            if btype == hf_parent_type:
                hf_parent_codes.append(code)
            elif btype == district_type:
                district_code_set.add(code)

    # Walk up the parent chain from each HF-parent boundary to find its district
    hf_to_district = {}
    for hf_code in hf_parent_codes:
        current = hf_code
        # Walk up at most 10 levels to avoid infinite loops
        for _ in range(10):
            parent = code_to_parent.get(current)
            if not parent:
                break
            if parent in district_code_set:
                hf_to_district[hf_code] = parent
                break
            current = parent

    return hf_to_district


def fill_parent_codes(facility_csv_path, output_csv_path, hf_mapping,
                      state_boundary_code,
                      hf_type_name="Health Facility",
                      district_type_name="District Facility"):
    """
    Fill parent_code in a facility CSV.

    - "Health Facility" rows: parent_code = district code from hf_mapping
    - "District Facility" rows: parent_code = comma-separated HF codes under
      that district + state_boundary_code

    Returns count of rows updated.
    """
    # Build inverse mapping: district_code -> list of HF codes
    district_to_hf = {}
    for hf_code, district_code in hf_mapping.items():
        district_to_hf.setdefault(district_code, []).append(hf_code)

    updated = 0

    with open(facility_csv_path, 'r', encoding='utf-8') as infile, \
         open(output_csv_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            facility_type = row.get('facility_type', '').strip()
            boundary_code = row.get('boundary_code', '').strip()

            if facility_type == hf_type_name:
                district_code = hf_mapping.get(boundary_code)
                if district_code:
                    row['parent_code'] = district_code
                    updated += 1

            elif facility_type == district_type_name:
                hf_codes = district_to_hf.get(boundary_code)
                if hf_codes:
                    row['parent_code'] = ",".join(hf_codes) + "," + state_boundary_code
                    updated += 1

            writer.writerow(row)

    return updated
