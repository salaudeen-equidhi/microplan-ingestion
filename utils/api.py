import json
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
