import json
import requests
import pandas as pd

# =============================================================================
# REPLACE THESE WITH YOUR TABLEAU ONLINE DETAILS
# =============================================================================
instance = "prod-apnortheast-a"
api_version = "3.24"
auth_url = f"https://{instance}.online.tableau.com/api/{api_version}/auth/signin"

token_name = "test"
token_value = ""
site_id = ""

auth_payload = {
    "credentials": {
        "personalAccessTokenName": token_name,
        "personalAccessTokenSecret": token_value,
        "site": {"contentUrl": site_id}
    }
}

auth_headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

# =============================================================================
# AUTHENTICATE AND OBTAIN TOKEN
# =============================================================================
try:
    response = requests.post(auth_url, json=auth_payload, headers=auth_headers)
    response.raise_for_status()
    data = response.json()
    auth_token = data['credentials']['token']
    print(f"Authenticated with token: {auth_token}")
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
    exit()

# =============================================================================
# METADATA API ENDPOINT & HEADERS
# =============================================================================
metadata_api_url = f"https://{instance}.online.tableau.com/api/metadata/graphql"
headers = {
    "Content-Type": "application/json",
    "X-Tableau-Auth": auth_token
}

# =============================================================================
# 1. GRAPHQL QUERY (INCLUDES upstreamFields FOR SHEET FIELDS)
# =============================================================================
def get_workbook_details(workbook_name):
    query = f"""
    {{
      workbooks(filter: {{name: "{workbook_name}"}}) {{
        id
        name
        dashboards {{
          name
          upstreamFields {{
            id
            name
            __typename
          }}
        }}
        projectName
        sheets {{
          id
          name
          __typename
          containedInDashboards {{
            name
          }}
          sheetFieldInstances {{
            name
            __typename
            id
            upstreamDatasources {{
              name
            }}
            upstreamDatabases {{
              name
            }}
            upstreamTables {{
              name
              schema
            }}
            upstreamColumns {{
              name
            }}
            upstreamFields {{
              id
              name
              __typename
            }}
          }}
        }}
      }}
    }}
    """
    response = requests.post(metadata_api_url, json={'query': query}, headers=headers)
    response.raise_for_status()
    return response.json()

# =============================================================================
# 2. GRAPHQL QUERY FOR BATCH FETCHING CALCULATEDFIELDS
# =============================================================================
def get_calculated_field_details(field_ids):
    if not field_ids:
        return {}
    id_within_str = '", "'.join(field_ids)
    id_within_filter = f'["{id_within_str}"]'
    query = f"""
    {{
      calculatedFields(filter: {{idWithin: {id_within_filter}}}) {{
        name
        id
        formula
        fields {{
          name
          id
          __typename
          upstreamTables {{
            name
          }}
          upstreamColumns {{
            name
          }}
          upstreamDatabases {{
            name
          }}
          upstreamFields {{
            id
            name
            __typename
          }}
        }}
      }}
    }}
    """
    response = requests.post(metadata_api_url, json={'query': query}, headers=headers)
    response.raise_for_status()
    return response.json()

# =============================================================================
# 3. RECURSIVE TRAVERSAL FOR CALCULATED FIELDS
# =============================================================================
def traverse_upstream_fields(current_field_id, calculated_fields_dict, lineage_rows, context, visited):
    """
    Recursive traversal of calculated fields, with duplicate prevention.
    """
    # Skip if already visited
    if current_field_id in visited:
        return
    visited.add(current_field_id)  # Mark this field as visited

    # If no data => unknown
    if current_field_id not in calculated_fields_dict:
        lineage_rows.append({
            "workbook_name": context["workbook_name"],
            "worksheet_name": context["sheet_name"],
            "data_source_name": context["data_source_name"],
            "dashboard_name": context["dashboard_name"],
            "field_name": context["parent_field_name"],
            "field_type": "CalculatedField",
            "upstream_field_name": "UNKNOWN",
            "upstream_field_type": "UNKNOWN",
            "formula": "",
            "upstream_column": "",
            "upstream_table": "",
            "upstream_schema": "",
            "upstream_database": ""
        })
        return

    cal_field_data = calculated_fields_dict[current_field_id]
    cal_field_name = cal_field_data["name"]
    cal_field_formula = cal_field_data["formula"]
    upstreams = cal_field_data.get("fields", [])

    # If no upstream => constant/noUpstream
    if not upstreams:
        lineage_rows.append({
            "workbook_name": context["workbook_name"],
            "worksheet_name": context["sheet_name"],
            "data_source_name": context["data_source_name"],
            "dashboard_name": context["dashboard_name"],
            "field_name": context["parent_field_name"],
            "field_type": "CalculatedField",
            "upstream_field_name": cal_field_name,
            "upstream_field_type": "Constant/NoUpstream",
            "formula": cal_field_formula,
            "upstream_column": "",
            "upstream_table": "",
            "upstream_schema": "",
            "upstream_database": ""
        })
        return

    # Process upstream fields
    for upstream_field in upstreams:
        up_type = upstream_field["__typename"]
        up_name = upstream_field["name"]
        up_id = upstream_field["id"]

        # Flatten upstream details (tables, columns, databases)
        up_tables = upstream_field.get("upstreamTables", [])
        up_cols = upstream_field.get("upstreamColumns", [])
        up_dbs = upstream_field.get("upstreamDatabases", [])

        # Add row for this upstream field
        lineage_rows.append({
            "workbook_name": context["workbook_name"],
            "worksheet_name": context["sheet_name"],
            "data_source_name": context["data_source_name"],
            "dashboard_name": context["dashboard_name"],
            "field_name": context["parent_field_name"],
            "field_type": "CalculatedField",
            "upstream_field_name": up_name,
            "upstream_field_type": up_type,
            "formula": cal_field_formula,
            "upstream_column": ", ".join(col["name"] for col in up_cols) if up_cols else "",
            "upstream_table": ", ".join(tbl["name"] for tbl in up_tables) if up_tables else "",
            "upstream_schema": "",
            "upstream_database": ", ".join(db["name"] for db in up_dbs) if up_dbs else ""
        })

        # Recursive traversal for calculated fields
        if up_type == "CalculatedField":
            new_context = context.copy()
            new_context["parent_field_name"] = up_name
            traverse_upstream_fields(up_id, calculated_fields_dict, lineage_rows, new_context, visited)

# =============================================================================
# 4. PROCESS SINGLE WORKBOOK (FIX DUPLICATES WITH VISITED TRACKING)
# =============================================================================
def process_single_workbook(wb_name):
    wb_json_data = get_workbook_details(wb_name)
    workbooks_data = wb_json_data.get("data", {}).get("workbooks", [])
    if not workbooks_data:
        print(f"No workbook found with name '{wb_name}'. Skipping.")
        return pd.DataFrame()

    workbook_obj = workbooks_data[0]
    workbook_name = workbook_obj["name"]

    # Gather calculated field IDs
    dashboards = workbook_obj.get("dashboards", [])
    calc_field_ids = []
    for dash in dashboards:
        for field in dash.get("upstreamFields", []):
            if field["__typename"] == "CalculatedField":
                calc_field_ids.append(field["id"])

    sheets = workbook_obj.get("sheets", [])
    for sheet in sheets:
        for sf in sheet.get("sheetFieldInstances", []):
            if sf["__typename"] == "DatasourceField":
                for uf in sf.get("upstreamFields", []):
                    if uf["__typename"] == "CalculatedField":
                        calc_field_ids.append(uf["id"])

    calc_field_ids = list(set(calc_field_ids))

    # Batch fetch calculated fields
    calculated_fields_details = get_calculated_field_details(calc_field_ids)
    calc_fields_data = calculated_fields_details.get("data", {}).get("calculatedFields", [])
    calc_field_lookup = {}
    for cfd in calc_fields_data:
        cfd_id = cfd["id"]
        calc_field_lookup[cfd_id] = {
            "name": cfd["name"],
            "formula": cfd["formula"],
            "fields": cfd.get("fields", [])
        }

    # Build lineage data
    lineage_data = []
    visited = set()  # Track visited fields to prevent duplicates
    for sheet in sheets:
        sheet_name = sheet["name"]
        dash_names = [d["name"] for d in sheet.get("containedInDashboards", [])] or ["NoDashboard"]

        for sf in sheet.get("sheetFieldInstances", []):
            field_name = sf["name"]
            field_type = sf["__typename"]
            field_id = sf["id"]

            upstream_datasources = sf.get("upstreamDatasources", [])
            upstream_tables = sf.get("upstreamTables", [])
            upstream_columns = sf.get("upstreamColumns", [])
            upstream_databases = sf.get("upstreamDatabases", [])
            ds_upstream_fields = sf.get("upstreamFields", []) or []

            for dash_name in dash_names:
                # Add primary field record
                lineage_data.append({
                    "workbook_name": workbook_name,
                    "worksheet_name": sheet_name,
                    "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                    "dashboard_name": dash_name,
                    "field_name": field_name,
                    "field_type": field_type,
                    "primary_field": True,  # Mark as primary
                    "upstream_field_name": "",
                    "upstream_field_type": "",
                    "formula": "",
                    "upstream_column": "",
                    "upstream_table": "",
                    "upstream_schema": "",
                    "upstream_database": ""
                })

                # Process upstream fields
                for uf in ds_upstream_fields:
                    if uf["__typename"] == "CalculatedField":
                        context = {
                            "workbook_name": workbook_name,
                            "sheet_name": sheet_name,
                            "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                            "dashboard_name": dash_name,
                            "parent_field_name": uf["name"]
                        }
                        traverse_upstream_fields(uf["id"], calc_field_lookup, lineage_data, context, visited)

    return pd.DataFrame(lineage_data)

# =============================================================================
# 5. MAIN
# =============================================================================
def main():
    with open("workbooks.txt", "r") as f:
        workbook_names = [line.strip() for line in f if line.strip()]

    with pd.ExcelWriter("lineage_output.xlsx", engine="openpyxl") as writer:
        wrote_any_data = False
        for wb_name in workbook_names:
            print(f"\nProcessing workbook: {wb_name}")
            df_lineage = process_single_workbook(wb_name)
            if df_lineage.empty:
                print(f" - No data found or workbook not found: {wb_name}")
                continue

            wrote_any_data = True
            safe_sheet_name = wb_name[:31] or "Sheet"
            df_lineage.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            print(f" - Wrote {len(df_lineage)} rows to '{safe_sheet_name}'.")

        if not wrote_any_data:
            dummy_df = pd.DataFrame({"No data found": []})
            dummy_df.to_excel(writer, sheet_name="EmptyResults", index=False)
            print(" - Created 'EmptyResults' sheet. No data for any workbook.")

    print("\n--- Done. Check lineage_output.xlsx. ---")


if __name__ == "__main__":
    main()
