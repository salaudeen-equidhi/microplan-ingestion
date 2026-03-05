import json
import os
from datetime import datetime

import pandas as pd
import requests


def ingest_boundary(api_url, tenant_id, project_type_id, csv_file_path):
    """
    Send a boundary CSV file to the DHIS2 ingestion API.

    Returns dict with keys: success, status_code, response
    """
    payload = {
        "DHIS2IngestionRequest": json.dumps({
            "tenantId": tenant_id,
            "requestInfo": {
                "userInfo": {
                    "id": 97,
                    "userName": project_type_id,
                    "name": project_type_id,
                    "mobileNumber": "9999999999",
                    "emailId": None,
                    "type": "EMPLOYEE",
                    "tenantId": tenant_id,
                    "roles": [
                        {"code": "SUPERVISOR", "name": "Supervisor", "tenantId": tenant_id},
                        {"code": "DISTRICT_SUPERVISOR", "name": "District Supervisor", "tenantId": tenant_id},
                        {"code": "SYSTEM_ADMINISTRATOR", "name": "System Administrator", "tenantId": tenant_id},
                        {"code": "SUPERUSER", "name": "Super User", "tenantId": tenant_id},
                        {"code": "NATIONAL_SUPERVISOR", "name": "National Supervisor", "tenantId": tenant_id},
                        {"code": "DISTRIBUTOR", "name": "Distributor", "tenantId": tenant_id},
                        {"code": "WAREHOUSE_MANAGER", "name": "Warehouse Manager", "tenantId": tenant_id},
                        {"code": "REGISTRAR", "name": "Registrar", "tenantId": tenant_id},
                        {"code": "PROVINCIAL_SUPERVISOR", "name": "Provincial Supervisor", "tenantId": tenant_id},
                    ],
                    "uuid": project_type_id
                }
            }
        })
    }

    headers = {"Accept": "application/json"}

    with open(csv_file_path, "rb") as f:
        files = [("file", ("file", f, "application/octet-stream"))]
        resp = requests.post(api_url, headers=headers, data=payload, files=files)

    return {
        "success": resp.ok,
        "status_code": resp.status_code,
        "response": resp.text,
    }


def ingest_facility(api_url, tenant_id, project_type_id, csv_file_path):
    """
    Send a facility CSV file to the facility ingestion API.

    Returns dict with keys: success, status_code, response
    """
    payload = {
        "DHIS2IngestionRequest": json.dumps({
            "tenantId": tenant_id,
            "dataType": "Facility",
            "requestInfo": {
                "userInfo": {
                    "id": 97,
                    "userName": project_type_id,
                    "salutation": None,
                    "name": "XYZ",
                    "gender": None,
                    "mobileNumber": "9999999999",
                    "emailId": None,
                    "altContactNumber": None,
                    "pan": None,
                    "aadhaarNumber": None,
                    "permanentAddress": None,
                    "permanentCity": None,
                    "permanentPinCode": None,
                    "correspondenceAddress": None,
                    "correspondenceCity": None,
                    "correspondencePinCode": None,
                    "alternatemobilenumber": None,
                    "active": True,
                    "locale": None,
                    "type": "EMPLOYEE",
                    "accountLocked": False,
                    "accountLockedDate": 0,
                    "fatherOrHusbandName": None,
                    "relationship": None,
                    "signature": None,
                    "bloodGroup": None,
                    "photo": None,
                    "identificationMark": None,
                    "createdBy": 23287,
                    "lastModifiedBy": 23287,
                    "tenantId": tenant_id,
                    "roles": [
                        {"code": "SUPERVISOR", "name": "Supervisor", "tenantId": tenant_id},
                        {"code": "DISTRICT_SUPERVISOR", "name": "District Supervisor", "tenantId": tenant_id},
                        {"code": "SYSTEM_ADMINISTRATOR", "name": "System Administrator", "tenantId": tenant_id},
                        {"code": "SUPERUSER", "name": "Super User", "tenantId": tenant_id},
                        {"code": "NATIONAL_SUPERVISOR", "name": "National Supervisor", "tenantId": tenant_id},
                        {"code": "DISTRIBUTOR", "name": "Distributor", "tenantId": tenant_id},
                        {"code": "WAREHOUSE_MANAGER", "name": "Warehouse Manager", "tenantId": tenant_id},
                        {"code": "REGISTRAR", "name": "Registrar", "tenantId": tenant_id},
                        {"code": "PROVINCIAL_SUPERVISOR", "name": "Provincial Supervisor", "tenantId": tenant_id},
                    ],
                    "uuid": project_type_id,
                    "createdDate": None,
                    "lastModifiedDate": None,
                    "dob": None,
                    "pwdExpiryDate": None,
                }
            }
        })
    }

    headers = {"Accept": "application/json"}

    with open(csv_file_path, "rb") as f:
        files = [("file", ("file", f, "application/octet-stream"))]
        resp = requests.post(api_url, headers=headers, data=payload, files=files)

    return {
        "success": resp.ok,
        "status_code": resp.status_code,
        "response": resp.text,
    }


DEFAULT_AUTH_TOKEN = '3a375f80-36ce-4b10-9437-97f7c74dc6dc'


def search_single_boundary(search_url, token=None, tenant_id=None, code=None, timeout=30):
    """Search for a single boundary code. Returns True if found, False otherwise."""
    if token is None:
        token = DEFAULT_AUTH_TOKEN
    payload = {
        "RequestInfo": {
            "apiId": "stribi",
            "ver": "stribi",
            "ts": 0,
            "action": "stribi",
            "did": "stribi",
            "key": "stribi",
            "msgId": "stribi",
            "requesterId": "stribi",
            "authToken": token,
            "userInfo": {"tenantId": tenant_id, "id": 0, "uuid": "stribi"},
        },
        "Boundary": [{"tenantId": tenant_id, "code": code, "geometry": None}],
    }
    try:
        resp = requests.post(search_url, json=payload, timeout=timeout)
        if not resp.ok:
            return False
        data = resp.json() if resp.text.strip() else {}
        boundaries = data.get("Boundary", [])
        code_lower = code.strip().lower()
        return any(
            isinstance(b, dict) and str(b.get("code", "")).strip().lower() == code_lower
            for b in boundaries
        )
    except Exception:
        return False


def verify_boundary_codes(search_url, token=None, tenant_id=None, codes=None, progress_cb=None):
    """
    Verify a list of boundary codes one at a time.

    Args:
        progress_cb: optional callback(current_index, total, code, found)
            called after each code is checked.

    Returns dict with keys: found_codes (set), not_found_codes (set),
        errors (int), total (int).
    """
    if token is None:
        token = DEFAULT_AUTH_TOKEN
    found_codes = set()
    not_found_codes = set()
    total = len(codes)

    for i, code in enumerate(codes):
        found = search_single_boundary(search_url, token, tenant_id, code)
        if found:
            found_codes.add(code)
        else:
            not_found_codes.add(code)
        if progress_cb:
            progress_cb(i + 1, total, code, found)

    return {
        "found_codes": found_codes,
        "not_found_codes": not_found_codes,
        "total": total,
    }


def search_facilities_batch(search_url, token=None, tenant_id=None, facility_names=None, timeout=60):
    """Search for facilities by clientReferenceId list. Returns set of found names."""
    if token is None:
        token = DEFAULT_AUTH_TOKEN
    if not facility_names:
        return set()
    payload = {
        "RequestInfo": {
            "apiId": "stribi",
            "ver": "stribi",
            "ts": 0,
            "action": "stribi",
            "did": "stribi",
            "key": "stribi",
            "msgId": "stribi",
            "requesterId": "stribi",
            "authToken": token,
            "userInfo": {"tenantId": tenant_id, "id": 0, "uuid": "stribi"},
        },
        "Facility": {
            "tenantid": tenant_id,
            "clientReferenceId": list(facility_names),
        },
    }
    headers = {"Content-Type": "application/json"}
    try:
        resp = requests.post(search_url, json=payload, headers=headers, timeout=timeout)
        if not resp.ok:
            return set()
        data = resp.json() if resp.text.strip() else {}
        facilities = data.get("Facilities", [])
        found = set()
        for f in facilities:
            if isinstance(f, dict):
                ref = str(f.get("clientReferenceId", "")).strip()
                if ref:
                    found.add(ref)
                    found.add(ref.lower())
        return found
    except Exception:
        return set()


def verify_facility_names(search_url, token=None, tenant_id=None, facility_names=None, progress_cb=None, batch_size=50):
    """
    Verify a list of facility names against the facility search API.

    Sends clientReferenceId as a list in batches.

    Args:
        progress_cb: optional callback(current_index, total, name, found)
        batch_size: number of names per API call

    Returns dict with keys: found_names (set), not_found_names (set), total (int).
    """
    if token is None:
        token = DEFAULT_AUTH_TOKEN

    found_names = set()
    not_found_names = set()
    total = len(facility_names)
    checked = 0

    for i in range(0, total, batch_size):
        batch = facility_names[i:i + batch_size]
        batch_found = search_facilities_batch(search_url, token, tenant_id, batch)

        for name in batch:
            checked += 1
            if name in batch_found or name.strip().lower() in batch_found:
                found_names.add(name)
                is_found = True
            else:
                not_found_names.add(name)
                is_found = False
            if progress_cb:
                progress_cb(checked, total, name, is_found)

    return {
        "found_names": found_names,
        "not_found_names": not_found_names,
        "total": total,
    }


def read_facility_names_from_csv(csv_path):
    """Read unique facility names from a CSV file."""
    df = _read_csv_with_fallback(csv_path)
    lowered = {str(c).strip().lower(): c for c in df.columns}
    col = None
    for candidate in ["facility_name", "facilityname", "name"]:
        if candidate in lowered:
            col = lowered[candidate]
            break
    if not col:
        raise ValueError(
            f'No facility_name column found in "{os.path.basename(csv_path)}". '
            "Expected a column like facility_name/name."
        )
    names = [str(v).strip() for v in df[col].tolist() if str(v).strip()]
    unique_names = list(dict.fromkeys(names))
    return col, unique_names


def generate_facility_ingestion_summary(
    csv_path, found_names, output_dir,
    name_column=None, status_column="INGESTION_STATUS",
):
    """Generate an Excel summary marking each facility as FOUND or NOT_FOUND."""
    df = _read_csv_with_fallback(csv_path)
    if not name_column:
        lowered = {str(c).strip().lower(): c for c in df.columns}
        for candidate in ["facility_name", "facilityname", "name"]:
            if candidate in lowered:
                name_column = lowered[candidate]
                break
    if not name_column:
        raise ValueError(
            f'No facility_name column found in "{os.path.basename(csv_path)}".'
        )

    normalized_found = {str(n).strip().lower() for n in found_names if str(n).strip()}
    name_values = df[name_column].astype(str).str.strip()

    df[status_column] = name_values.apply(
        lambda n: "FOUND" if n and n.lower() in normalized_found else "NOT_FOUND"
    )

    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}_facility_summary_{ts}.xlsx")
    df.to_excel(output_path, index=False)

    total_rows = len(df.index)
    found_rows = int((df[status_column] == "FOUND").sum())
    not_found_rows = total_rows - found_rows
    return {
        "output_path": output_path,
        "name_column": name_column,
        "total_rows": total_rows,
        "found_rows": found_rows,
        "not_found_rows": not_found_rows,
    }


def _read_csv_with_fallback(csv_path):
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    last_error = None
    for encoding in encodings:
        try:
            return pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding=encoding)
        except Exception as exc:
            last_error = exc
    raise ValueError(f"Could not read CSV {csv_path}: {last_error}")


def detect_code_column(df):
    if "code" in df.columns:
        return "code"

    lowered = {str(c).strip().lower(): c for c in df.columns}
    for candidate in ["boundarycode", "boundary_code", "code"]:
        if candidate in lowered:
            return lowered[candidate]

    for col in df.columns:
        name = str(col).strip().lower()
        if "code" in name:
            return col
    return None


def read_boundary_codes_from_csv(csv_path, code_column=None):
    df = _read_csv_with_fallback(csv_path)
    selected_code_col = code_column or detect_code_column(df)
    if not selected_code_col:
        raise ValueError(
            f'No boundary code column found in "{os.path.basename(csv_path)}". '
            "Expected a column like code/boundary_code."
        )

    codes = [
        str(v).strip() for v in df[selected_code_col].tolist()
        if str(v).strip()
    ]
    unique_codes = list(dict.fromkeys(codes))
    return selected_code_col, unique_codes


def generate_boundary_ingestion_summary(
    csv_path,
    found_codes,
    output_dir,
    code_column=None,
    status_column="INGESTION_STATUS",
    deduplicate_by_code=True,
):
    df = _read_csv_with_fallback(csv_path)
    selected_code_col = code_column or detect_code_column(df)
    if not selected_code_col:
        raise ValueError(
            f'No boundary code column found in "{os.path.basename(csv_path)}". '
            "Expected a column like code/boundary_code."
        )

    normalized_found = {str(code).strip().lower() for code in found_codes if str(code).strip()}
    code_values = df[selected_code_col].astype(str).str.strip()
    if deduplicate_by_code:
        df[selected_code_col] = code_values
        df = df[df[selected_code_col] != ""].drop_duplicates(subset=[selected_code_col], keep="first")
        code_values = df[selected_code_col].astype(str).str.strip()

    df[status_column] = code_values.apply(
        lambda code: "FOUND" if code and code.lower() in normalized_found else "NOT_FOUND"
    )

    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}_boundary_summary_{ts}.xlsx")
    df.to_excel(output_path, index=False)

    total_rows = len(df.index)
    found_rows = int((df[status_column] == "FOUND").sum())
    not_found_rows = total_rows - found_rows
    return {
        "output_path": output_path,
        "code_column": selected_code_col,
        "total_rows": total_rows,
        "found_rows": found_rows,
        "not_found_rows": not_found_rows,
    }
