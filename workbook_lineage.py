import json
import requests
import pandas as pd

# =============================================================================
# REPLACE THESE VALUES WITH YOUR ACTUAL TABLEAU ONLINE DETAILS
# =============================================================================
instance = ""
api_version = "3.24"
auth_url = f"https://{instance}.online.tableau.com/api/{api_version}/auth/signin"

token_name = ""
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
# SETUP METADATA API ENDPOINT AND HEADERS
# =============================================================================
metadata_api_url = f"https://{instance}.online.tableau.com/api/metadata/graphql"
headers = {
    "Content-Type": "application/json",
    "X-Tableau-Auth": auth_token
}

# =============================================================================
# FUNCTIONS FOR METADATA API QUERIES
# =============================================================================

def get_workbook_details(workbook_name):
    """
    Returns JSON data for the specified workbook name, including dashboards,
    sheet fields, upstream columns, etc.
    """
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
                }}
            }}
        }}
    }}
    """
    response = requests.post(metadata_api_url, json={'query': query}, headers=headers)
    response.raise_for_status()
    return response.json()


def get_calculated_field_details(field_ids):
    """
    Returns JSON data of CalculatedFields, including formula and each field's
    upstream fields (which may themselves be CalculatedField or DatasourceField).
    """
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
        }}
      }}
    }}
    """
    response = requests.post(metadata_api_url, json={'query': query}, headers=headers)
    response.raise_for_status()
    return response.json()

# =============================================================================
# HELPER RECURSIVE FUNCTION FOR CALCULATED FIELD LINEAGE
# =============================================================================

def traverse_upstream_fields(current_field_id, calculated_fields_dict, lineage_rows, context):
    """
    Traverse upstream fields of a CalculatedField in a depth-first manner:
    - If the upstream field is another CalculatedField, keep traversing.
    - If the upstream field is a DataSourceField, record final lineage info.
    - If no upstream fields exist, record the direct formula-only info (constants).
    
    context contains: 
        "workbook_name", "sheet_name", "data_source_name", "dashboard_name", "parent_field_name"
    """
    # If we don't have the current_field_id in our dict, record as UNKNOWN
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

    # If no upstream fields => might be a constant or no direct upstream
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

    # Otherwise, iterate each upstream field
    for upstream_field in upstreams:
        up_field_type = upstream_field["__typename"]
        up_field_name = upstream_field["name"]
        up_field_id = upstream_field["id"]

        if up_field_type == "CalculatedField":
            # 1. Record immediate link
            lineage_rows.append({
                "workbook_name": context["workbook_name"],
                "worksheet_name": context["sheet_name"],
                "data_source_name": context["data_source_name"],
                "dashboard_name": context["dashboard_name"],
                "field_name": context["parent_field_name"],
                "field_type": "CalculatedField",
                "upstream_field_name": up_field_name,
                "upstream_field_type": "CalculatedField",
                "formula": cal_field_formula,
                "upstream_column": "",
                "upstream_table": "",
                "upstream_schema": "",
                "upstream_database": ""
            })

            # 2. Recursively explore upstream
            new_context = context.copy()
            new_context["parent_field_name"] = up_field_name
            traverse_upstream_fields(up_field_id, calculated_fields_dict, lineage_rows, new_context)

        elif up_field_type == "DatasourceField":
            # Possibly multiple upstream tables/columns/db
            upstream_tables = upstream_field.get("upstreamTables", [])
            upstream_columns = upstream_field.get("upstreamColumns", [])
            upstream_databases = upstream_field.get("upstreamDatabases", [])

            if not (upstream_tables or upstream_columns or upstream_databases):
                lineage_rows.append({
                    "workbook_name": context["workbook_name"],
                    "worksheet_name": context["sheet_name"],
                    "data_source_name": context["data_source_name"],
                    "dashboard_name": context["dashboard_name"],
                    "field_name": context["parent_field_name"],
                    "field_type": "CalculatedField",
                    "upstream_field_name": up_field_name,
                    "upstream_field_type": "DatasourceField",
                    "formula": cal_field_formula,
                    "upstream_column": "",
                    "upstream_table": "",
                    "upstream_schema": "",
                    "upstream_database": ""
                })
            else:
                # Flatten multiple references
                for tbl in (upstream_tables or [None]):
                    for col in (upstream_columns or [None]):
                        for db in (upstream_databases or [None]):
                            lineage_rows.append({
                                "workbook_name": context["workbook_name"],
                                "worksheet_name": context["sheet_name"],
                                "data_source_name": context["data_source_name"],
                                "dashboard_name": context["dashboard_name"],
                                "field_name": context["parent_field_name"],
                                "field_type": "CalculatedField",
                                "upstream_field_name": up_field_name,
                                "upstream_field_type": "DatasourceField",
                                "formula": cal_field_formula,
                                "upstream_column": col["name"] if col else "",
                                "upstream_table": tbl["name"] if tbl else "",
                                "upstream_schema": "",
                                "upstream_database": db["name"] if db else ""
                            })
        else:
            # Some other potential typename
            lineage_rows.append({
                "workbook_name": context["workbook_name"],
                "worksheet_name": context["sheet_name"],
                "data_source_name": context["data_source_name"],
                "dashboard_name": context["dashboard_name"],
                "field_name": context["parent_field_name"],
                "field_type": "CalculatedField",
                "upstream_field_name": up_field_name,
                "upstream_field_type": up_field_type,
                "formula": cal_field_formula,
                "upstream_column": "",
                "upstream_table": "",
                "upstream_schema": "",
                "upstream_database": ""
            })


def process_single_workbook(wb_name):
    """
    Returns a pandas DataFrame containing the lineage info for the given workbook name.
    """
    wb_json_data = get_workbook_details(wb_name)

    workbooks_data = wb_json_data.get("data", {}).get("workbooks", [])
    if not workbooks_data:
        print(f"No workbook found with name '{wb_name}'. Skipping.")
        return pd.DataFrame()  # Return empty dataframe

    workbook_obj = workbooks_data[0]
    workbook_name = workbook_obj["name"]

    # Collect CalcField IDs from dashboards
    dashboards = workbook_obj.get("dashboards", [])
    calc_field_ids = []
    for dash in dashboards:
        for field in dash.get("upstreamFields", []):
            if field["__typename"] == "CalculatedField":
                calc_field_ids.append(field["id"])
    calc_field_ids = list(set(calc_field_ids))

    # Fetch details for all CalculatedFields in one shot
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

    # Now gather lineage rows
    lineage_data = []
    sheets = workbook_obj.get("sheets", [])
    for sheet in sheets:
        sheet_name = sheet["name"]
        contained_dashboards = sheet.get("containedInDashboards", [])
        dash_names = [d["name"] for d in contained_dashboards] or ["NoDashboard"]

        sheet_fields = sheet.get("sheetFieldInstances", [])
        for sf in sheet_fields:
            field_name = sf["name"]
            field_type = sf["__typename"]
            field_id = sf["id"]

            upstream_datasources = sf.get("upstreamDatasources", [])
            upstream_tables = sf.get("upstreamTables", [])
            upstream_columns = sf.get("upstreamColumns", [])
            upstream_databases = sf.get("upstreamDatabases", [])

            for dash_name in dash_names:
                if field_type == "DatasourceField":
                    # Possibly multiple upstream tables/columns/databases
                    if not (upstream_tables or upstream_columns or upstream_databases):
                        lineage_data.append({
                            "workbook_name": workbook_name,
                            "worksheet_name": sheet_name,
                            "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                            "dashboard_name": dash_name,
                            "field_name": field_name,
                            "field_type": "DatasourceField",
                            "upstream_field_name": "",
                            "upstream_field_type": "",
                            "formula": "",
                            "upstream_column": "",
                            "upstream_table": "",
                            "upstream_schema": "",
                            "upstream_database": ""
                        })
                    else:
                        # Flatten references
                        for tbl in (upstream_tables or [None]):
                            for col in (upstream_columns or [None]):
                                for db in (upstream_databases or [None]):
                                    lineage_data.append({
                                        "workbook_name": workbook_name,
                                        "worksheet_name": sheet_name,
                                        "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                                        "dashboard_name": dash_name,
                                        "field_name": field_name,
                                        "field_type": "DatasourceField",
                                        "upstream_field_name": "",
                                        "upstream_field_type": "",
                                        "formula": "",
                                        "upstream_column": col["name"] if col else "",
                                        "upstream_table": tbl["name"] if tbl else "",
                                        "upstream_schema": tbl["schema"] if tbl else "",
                                        "upstream_database": db["name"] if db else ""
                                    })

                elif field_type == "CalculatedField":
                    # Look up from calc_field_lookup
                    if field_id not in calc_field_lookup:
                        # Possibly a calc field that doesn't appear in the dash upstreamFields
                        lineage_data.append({
                            "workbook_name": workbook_name,
                            "worksheet_name": sheet_name,
                            "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                            "dashboard_name": dash_name,
                            "field_name": field_name,
                            "field_type": "CalculatedField",
                            "upstream_field_name": "UNKNOWN",
                            "upstream_field_type": "UNKNOWN",
                            "formula": "",
                            "upstream_column": "",
                            "upstream_table": "",
                            "upstream_schema": "",
                            "upstream_database": ""
                        })
                    else:
                        context = {
                            "workbook_name": workbook_name,
                            "sheet_name": sheet_name,
                            "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                            "dashboard_name": dash_name,
                            "parent_field_name": field_name
                        }
                        traverse_upstream_fields(field_id, calc_field_lookup, lineage_data, context)

                else:
                    # Some other type
                    lineage_data.append({
                        "workbook_name": workbook_name,
                        "worksheet_name": sheet_name,
                        "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                        "dashboard_name": dash_name,
                        "field_name": field_name,
                        "field_type": field_type,
                        "upstream_field_name": "",
                        "upstream_field_type": "",
                        "formula": "",
                        "upstream_column": "",
                        "upstream_table": "",
                        "upstream_schema": "",
                        "upstream_database": ""
                    })

    # Create DataFrame
    df_lineage = pd.DataFrame(lineage_data)
    # Deduplicate exact matches
    df_lineage.drop_duplicates(inplace=True)

    return df_lineage


def main():
    # Read workbook names from file
    with open('workbooks.txt', 'r') as f:
        workbook_names = [line.strip() for line in f if line.strip()]

    # Create a single Excel file, multiple tabs
    with pd.ExcelWriter("lineage_output.xlsx", engine="openpyxl") as writer:
        for wb_name in workbook_names:
            print(f"\nProcessing workbook: {wb_name}")
            df_lineage = process_single_workbook(wb_name)

            if df_lineage.empty:
                # No data found or workbook not found
                continue

            # Excel sheet names have a 31 char limit
            safe_sheet_name = wb_name[:31] or "Sheet"
            df_lineage.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            print(f" - Wrote {len(df_lineage)} rows to sheet '{safe_sheet_name}'")

    print("\n--- Done. See 'lineage_output.xlsx' for results. ---")

if __name__ == "__main__":
    main()
