import json
import requests
import pandas as pd

# =============================================================================
# REPLACE THE BELOW VALUES WITH YOUR ACTUAL TABLEAU ONLINE DETAILS
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
# FUNCTION TO GET WORKBOOK DETAILS (DASHBOARDS, SHEETS, ETC.)
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

# =============================================================================
# FUNCTION TO GET CALCULATED FIELD DETAILS IN BATCH
# =============================================================================
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
# HELPER FUNCTION TO ITERATE THROUGH A CALCULATED FIELD'S UPSTREAM FIELDS
# (RECURSIVE OR STACK-BASED APPROACH)
# =============================================================================
def traverse_upstream_fields(
    current_field_id,
    calculated_fields_dict,
    lineage_rows,
    context
):
    """
    Traverse upstream fields of a CalculatedField in a depth-first manner:
    - If the upstream field is another CalculatedField, keep traversing.
    - If the upstream field is a DataSourceField, record final lineage info.
    - If no upstream fields exist, record the direct formula-only info (constants).
    
    Parameters:
    -----------
    current_field_id : str
        The unique ID of the current CalculatedField to be explored.
    calculated_fields_dict : dict
        {calculated_field_id: { 'name':..., 'formula':..., 'fields': [...] }}
    lineage_rows : list
        A list to which we append dictionary rows of lineage
    context : dict
        Contains context like workbook_name, sheet_name, dashboard_name, parent_field_name, ...
    
    Returns:
    --------
    None (updates lineage_rows in place)
    """
    # If we don't have the current_field_id in the dictionary, record as "UNKNOWN"
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

    # If no upstream fields => likely a constant or no direct upstream
    if not upstreams:
        lineage_rows.append({
            "workbook_name": context["workbook_name"],
            "worksheet_name": context["sheet_name"],
            "data_source_name": context["data_source_name"],
            "dashboard_name": context["dashboard_name"],
            "field_name": context["parent_field_name"],  # e.g. cal1
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
            # 1. Record the immediate link (cal1 -> cal2)
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

            # 2. Recursively explore the upstream CalculatedField
            new_context = context.copy()
            new_context["parent_field_name"] = up_field_name
            traverse_upstream_fields(up_field_id, calculated_fields_dict, lineage_rows, new_context)

        elif up_field_type == "DatasourceField":
            # Gather possible table/column/database info
            upstream_tables = upstream_field.get("upstreamTables", [])
            upstream_columns = upstream_field.get("upstreamColumns", [])
            upstream_databases = upstream_field.get("upstreamDatabases", [])

            # If there's nothing, just record a single row
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
                # Flatten multiple upstream table/column/database references
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
            # Another potential typename we haven't explicitly handled
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

# =============================================================================
# MAIN SCRIPT ENTRY
# =============================================================================
def main():
    # Read workbook names from a text file
    with open('workbooks.txt', 'r') as f:
        workbook_names = [line.strip() for line in f if line.strip()]

    # Prepare a master list to hold lineage data across all workbooks
    all_lineage_data = []

    for wb_name in workbook_names:
        print(f"\nProcessing workbook: {wb_name}")
        wb_json_data = get_workbook_details(wb_name)

        workbooks_data = wb_json_data.get("data", {}).get("workbooks", [])
        if not workbooks_data:
            print(f"No workbook found with name '{wb_name}'. Skipping.")
            continue

        workbook_obj = workbooks_data[0]
        workbook_name = workbook_obj["name"]

        # ---------------------------------------------------------------------
        # 1. Collect all CalculatedField IDs from dashboards (upstreamFields)
        # ---------------------------------------------------------------------
        dashboards = workbook_obj.get("dashboards", [])
        calc_field_ids = []
        for dash in dashboards:
            for field in dash.get("upstreamFields", []):
                if field["__typename"] == "CalculatedField":
                    calc_field_ids.append(field["id"])
        # De-duplicate
        calc_field_ids = list(set(calc_field_ids))

        # ---------------------------------------------------------------------
        # 2. Query the list of all CalculatedFields in one shot
        # ---------------------------------------------------------------------
        calculated_fields_details = get_calculated_field_details(calc_field_ids)
        # Build a dictionary for quick lookup:
        calc_fields_data = calculated_fields_details.get("data", {}).get("calculatedFields", [])
        calc_field_lookup = {}
        for cfd in calc_fields_data:
            cfd_id = cfd["id"]
            calc_field_lookup[cfd_id] = {
                "name": cfd["name"],
                "formula": cfd["formula"],
                "fields": cfd.get("fields", [])
            }

        # ---------------------------------------------------------------------
        # 3. Process each Sheet, gather sheetFieldInstances
        # ---------------------------------------------------------------------
        sheets = workbook_obj.get("sheets", [])
        for sheet in sheets:
            sheet_name = sheet["name"]
            contained_dashboards = sheet.get("containedInDashboards", [])
            # A sheet can be in multiple dashboards:
            dash_names = [d["name"] for d in contained_dashboards] or ["NoDashboard"]

            sheet_fields = sheet.get("sheetFieldInstances", [])

            for sf in sheet_fields:
                field_name = sf["name"]
                field_type = sf["__typename"]
                field_id = sf["id"]

                # Upstream info from the sheetFieldInstance
                upstream_datasources = sf.get("upstreamDatasources", [])
                upstream_tables = sf.get("upstreamTables", [])
                upstream_columns = sf.get("upstreamColumns", [])
                upstream_databases = sf.get("upstreamDatabases", [])

                # We'll produce a record for each dashboard referencing this sheet
                for dash_name in dash_names:
                    if field_type == "DatasourceField":
                        # Possibly multiple upstream tables/columns/databases
                        if not (upstream_tables or upstream_columns or upstream_databases):
                            # If none, push a single row with blanks
                            all_lineage_data.append({
                                "workbook_name": workbook_name,
                                "worksheet_name": sheet_name,
                                "data_source_name": ", ".join([ds["name"] for ds in upstream_datasources]),
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
                            # Flatten them, one row per combination
                            for tbl in (upstream_tables or [None]):
                                for col in (upstream_columns or [None]):
                                    for db in (upstream_databases or [None]):
                                        all_lineage_data.append({
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
                        # Look up details from 'calc_field_lookup'
                        if field_id not in calc_field_lookup:
                            # Possibly a calc field that doesn't appear in the dashboard upstreamFields
                            all_lineage_data.append({
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
                            # Prepare context
                            context = {
                                "workbook_name": workbook_name,
                                "sheet_name": sheet_name,
                                "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                                "dashboard_name": dash_name,
                                "parent_field_name": field_name
                            }
                            # Recursively expand the lineage
                            traverse_upstream_fields(
                                current_field_id=field_id,
                                calculated_fields_dict=calc_field_lookup,
                                lineage_rows=all_lineage_data,
                                context=context
                            )
                    else:
                        # Some other field type
                        all_lineage_data.append({
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

    # =============================================================================
    # Convert all_lineage_data into a DataFrame and DEDUPLICATE
    # =============================================================================
    df_lineage = pd.DataFrame(all_lineage_data)

    # Drop fully identical rows to reduce duplicates. 
    # If you need to be more selective, you can pass subset=[...] with specific columns.
    df_lineage.drop_duplicates(inplace=True)

    print("\n--- FINAL LINEAGE DATA (DEDUPLICATED) ---")
    print(df_lineage)

    # If you want to export to CSV:
    # df_lineage.to_csv("lineage_output.csv", index=False)

if __name__ == "__main__":
    main()
