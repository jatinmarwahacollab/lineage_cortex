import json
import requests
import pandas as pd

# =============================================================================
# REPLACE THE BELOW VALUES WITH YOUR ACTUAL TABLEAU ONLINE DETAILS
# =============================================================================
instance = "prod-apnortheast-a"
api_version = "3.24"
auth_url = f"https://{instance}.online.tableau.com/api/{api_version}/auth/signin"

token_name = "test"
token_value = "Hndlty4pQCqbj+PLI2PtYA==:cl3eINuXnWV3LHraUQg9fQSweQy3NLXk"
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
    - If no upstream fields exist, record the direct formula-only info (e.g. constants).
    
    Parameters:
    -----------
    current_field_id : str
        The unique ID of the current CalculatedField to be explored.
    calculated_fields_dict : dict
        A dict of {calculated_field_id: { name, formula, fields[...]}} for quick lookup
    lineage_rows : list
        A list to which we append dictionary rows of lineage
    context : dict
        Contains context like workbook_name, sheet_name, dashboard_name, parent_field_name, ...
    
    Returns:
    --------
    None (updates lineage_rows in place)
    """
    # Basic sanity check
    if current_field_id not in calculated_fields_dict:
        # This might happen if the field_id is not truly a CalculatedField
        # or if we have no data. We'll record a single row with "Unknown"
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

    # Each CalculatedField has a "fields" array. Each element can be either:
    #  - Another CalculatedField (via __typename=="CalculatedField")
    #  - A DatasourceField (via __typename=="DatasourceField")
    #  - Something else if relevant
    # If empty, it might be a constant expression or has no upstream fields.
    upstreams = cal_field_data.get("fields", [])

    # If no upstream fields => This is likely a constant or does not rely on any upstream fields.
    if not upstreams:
        lineage_rows.append({
            "workbook_name": context["workbook_name"],
            "worksheet_name": context["sheet_name"],
            "data_source_name": context["data_source_name"],
            "dashboard_name": context["dashboard_name"],
            "field_name": context["parent_field_name"],         # e.g. cal1
            "field_type": "CalculatedField",
            "upstream_field_name": cal_field_name,              # same as itself, or we can put "None"
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
            # 1. Record the immediate link (parent -> upstream)
            lineage_rows.append({
                "workbook_name": context["workbook_name"],
                "worksheet_name": context["sheet_name"],
                "data_source_name": context["data_source_name"],
                "dashboard_name": context["dashboard_name"],
                "field_name": context["parent_field_name"],  # e.g. cal1
                "field_type": "CalculatedField",
                "upstream_field_name": up_field_name,         # e.g. cal2
                "upstream_field_type": "CalculatedField",
                "formula": cal_field_formula,
                "upstream_column": "",
                "upstream_table": "",
                "upstream_schema": "",
                "upstream_database": ""
            })

            # 2. Recursively explore that upstream CalculatedField
            new_context = context.copy()
            new_context["parent_field_name"] = up_field_name  # now parent field is cal2
            traverse_upstream_fields(up_field_id, calculated_fields_dict, lineage_rows, new_context)

        elif up_field_type == "DatasourceField":
            # 1. Record the immediate link (parent -> upstream datasource field)
            # 2. Also capture upstreamTables, upstreamColumns, upstreamDatabases if present
            #    In the 'get_calculated_field_details' result, these are directly in the field object
            #    for the upstream. We can parse them here.

            # Attempt to get tables, columns, databases
            upstream_tables = upstream_field.get("upstreamTables", [])
            upstream_columns = upstream_field.get("upstreamColumns", [])
            upstream_databases = upstream_field.get("upstreamDatabases", [])

            # There can be multiple upstream tables, columns, databases. You can decide how to store them.
            # For simplicity, let's flatten them in separate rows if multiple exist.
            # If you prefer to store them as a comma-separated string, adapt below.

            if not (upstream_tables or upstream_columns or upstream_databases):
                # If there's nothing, we'll just record one row with blank columns
                lineage_rows.append({
                    "workbook_name": context["workbook_name"],
                    "worksheet_name": context["sheet_name"],
                    "data_source_name": context["data_source_name"],
                    "dashboard_name": context["dashboard_name"],
                    "field_name": context["parent_field_name"],
                    "field_type": "CalculatedField",
                    "upstream_field_name": up_field_name,  # datasource field name
                    "upstream_field_type": "DatasourceField",
                    "formula": cal_field_formula,
                    "upstream_column": "",
                    "upstream_table": "",
                    "upstream_schema": "",
                    "upstream_database": ""
                })
            else:
                # If we have multiple tables/columns/db, let's produce multiple rows
                for table_obj in (upstream_tables or [None]):
                    for col_obj in (upstream_columns or [None]):
                        for db_obj in (upstream_databases or [None]):
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
                                "upstream_column": col_obj["name"] if col_obj else "",
                                "upstream_table": table_obj["name"] if table_obj else "",
                                "upstream_schema": "",
                                "upstream_database": db_obj["name"] if db_obj else ""
                            })
        else:
            # Possibly a different typename, handle generically:
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

        # "workbooks" key from the returned JSON
        workbooks_data = wb_json_data.get("data", {}).get("workbooks", [])
        if not workbooks_data:
            print(f"No workbook found with name '{wb_name}'. Skipping.")
            continue

        # Typically, there might be multiple workbooks with same name, but let's assume unique
        workbook_obj = workbooks_data[0]
        workbook_name = workbook_obj["name"]

        # ---------------------------------------------------------------------
        # Step 1: Collect all CalculatedField IDs from dashboards (upstreamFields)
        # ---------------------------------------------------------------------
        dashboards = workbook_obj.get("dashboards", [])
        calc_field_ids = []
        for dash in dashboards:
            dash_name = dash["name"]
            for field in dash.get("upstreamFields", []):
                if field["__typename"] == "CalculatedField":
                    calc_field_ids.append(field["id"])

        # De-duplicate
        calc_field_ids = list(set(calc_field_ids))

        # ---------------------------------------------------------------------
        # Step 2: Query the list of all CalculatedFields in one shot
        # ---------------------------------------------------------------------
        calculated_fields_details = get_calculated_field_details(calc_field_ids)
        # We'll build a dictionary for quick lookup: {calculated_field_id -> {name, formula, fields: [...]} }
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
        # Step 3: Process each Sheet, gather sheetFieldInstances
        # ---------------------------------------------------------------------
        sheets = workbook_obj.get("sheets", [])
        for sheet in sheets:
            sheet_name = sheet["name"]
            contained_dashboards = sheet.get("containedInDashboards", [])
            # A sheet can be in multiple dashboards, but let's process them
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

                # We'll produce at least one record for each dashboard that references this sheet
                for dash_name in dash_names:
                    # If this is a DatasourceField, we can directly record
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
                            # Flatten them, one row per combination of table/column/database
                            for tbl in (upstream_tables or [None]):
                                for col in (upstream_columns or [None]):
                                    for db in (upstream_databases or [None]):
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
                                            "upstream_column": col["name"] if col else "",
                                            "upstream_table": tbl["name"] if tbl else "",
                                            "upstream_schema": tbl["schema"] if tbl else "",
                                            "upstream_database": db["name"] if db else ""
                                        })

                    elif field_type == "CalculatedField":
                        # We need to go look up the details from 'calc_field_lookup' for this field_id
                        # and recursively gather dependencies
                        if field_id not in calc_field_lookup:
                            # Possibly, it's a calculated field that doesn't appear in the dashboard upstreamFields
                            # We'll record a single row with minimal info
                            all_lineage_data.append({
                                "workbook_name": workbook_name,
                                "worksheet_name": sheet_name,
                                "data_source_name": ", ".join([ds["name"] for ds in upstream_datasources]),
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
                                "data_source_name": ", ".join([ds["name"] for ds in upstream_datasources]),
                                "dashboard_name": dash_name,
                                "parent_field_name": field_name  # the current field
                            }
                            # Recursively expand
                            traverse_upstream_fields(
                                current_field_id=field_id,
                                calculated_fields_dict=calc_field_lookup,
                                lineage_rows=all_lineage_data,
                                context=context
                            )
                    else:
                        # Some other type of field
                        all_lineage_data.append({
                            "workbook_name": workbook_name,
                            "worksheet_name": sheet_name,
                            "data_source_name": ", ".join([ds["name"] for ds in upstream_datasources]),
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
    # Convert all_lineage_data into a DataFrame for easy manipulation
    # =============================================================================
    df_lineage = pd.DataFrame(all_lineage_data)
    print("\n--- FINAL LINEAGE DATA ---")
    print(df_lineage)

    # If you want, you can export to CSV:
    df_lineage.to_csv("lineage_output.csv", index=False)

if __name__ == "__main__":
    main()
