import json
import requests
import pandas as pd

# =============================================================================
# REPLACE THESE VALUES WITH YOUR ACTUAL TABLEAU ONLINE DETAILS
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
# 1. ENHANCED GRAPHQL QUERY FOR WORKBOOK (ADD upstreamFields FOR SHEET FIELDS)
# =============================================================================
def get_workbook_details(workbook_name):
    """
    Returns JSON data for the specified workbook name, including dashboards,
    sheet fields, upstream columns, plus upstreamFields for each sheet field 
    so we can see if a 'DatasourceField' actually references a 'CalculatedField' or 'ColumnField'.
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
                    # IMPORTANT: Add upstreamFields to handle the "DatasourceField as Calc" scenario
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
# 2. BATCH QUERY FOR CALCULATEDFIELDS
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
        }}
      }}
    }}
    """
    response = requests.post(metadata_api_url, json={'query': query}, headers=headers)
    response.raise_for_status()
    return response.json()

# =============================================================================
# 3. RECURSIVE TRAVERSAL FOR CALCULATED FIELD DEPENDENCIES
# =============================================================================
def traverse_upstream_fields(current_field_id, calculated_fields_dict, lineage_rows, context):
    """
    Traverse upstream fields of a CalculatedField in depth-first manner:
    - If the upstream field is another CalculatedField, keep traversing.
    - If the upstream field is a DatasourceField, record final lineage info.
    - If no upstream fields exist, it's likely a constant or no direct upstream.
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

# =============================================================================
# 4. PROCESS SINGLE WORKBOOK
# =============================================================================
def process_single_workbook(wb_name):
    wb_json_data = get_workbook_details(wb_name)
    workbooks_data = wb_json_data.get("data", {}).get("workbooks", [])
    if not workbooks_data:
        print(f"No workbook found with name '{wb_name}'. Skipping.")
        return pd.DataFrame()

    workbook_obj = workbooks_data[0]
    workbook_name = workbook_obj["name"]

    # -------------------------------------------------------------------------
    # A) Collect CalcField IDs from dashboards (as before)
    # -------------------------------------------------------------------------
    dashboards = workbook_obj.get("dashboards", [])
    calc_field_ids = []
    for dash in dashboards:
        for field in dash.get("upstreamFields", []):
            if field["__typename"] == "CalculatedField":
                calc_field_ids.append(field["id"])

    # -------------------------------------------------------------------------
    # B) Also gather CalcField IDs if a 'sheetFieldInstance' is DataSourceField 
    #    referencing a CalculatedField in its upstreamFields
    # -------------------------------------------------------------------------
    sheets = workbook_obj.get("sheets", [])
    for sheet in sheets:
        sheet_fields = sheet.get("sheetFieldInstances", [])
        for sf in sheet_fields:
            if sf["__typename"] == "DatasourceField":
                # If a DataSourceField itself references a CalculatedField upstream
                uf_list = sf.get("upstreamFields", []) or []
                for uf in uf_list:
                    if uf["__typename"] == "CalculatedField":
                        calc_field_ids.append(uf["id"])

    calc_field_ids = list(set(calc_field_ids))

    # -------------------------------------------------------------------------
    # C) Fetch details for all CalculatedFields in one shot
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # D) Build lineage
    # -------------------------------------------------------------------------
    lineage_data = []
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

            # Also any 'upstreamFields' for a DataSourceField
            ds_upstream_fields = sf.get("upstreamFields", []) or []

            for dash_name in dash_names:
                if field_type == "DatasourceField":
                    # 1) Check if it references a CalculatedField in 'upstreamFields'
                    if ds_upstream_fields:
                        # For each upstream field
                        for uf in ds_upstream_fields:
                            uf_type = uf["__typename"]
                            uf_name = uf["name"]
                            uf_id = uf["id"]

                            if uf_type == "CalculatedField":
                                # Record immediate link
                                lineage_data.append({
                                    "workbook_name": workbook_name,
                                    "worksheet_name": sheet_name,
                                    "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                                    "dashboard_name": dash_name,
                                    "field_name": field_name,
                                    "field_type": "DatasourceField",
                                    "upstream_field_name": uf_name,
                                    "upstream_field_type": "CalculatedField",
                                    "formula": calc_field_lookup.get(uf_id, {}).get("formula", ""),
                                    "upstream_column": "",
                                    "upstream_table": "",
                                    "upstream_schema": "",
                                    "upstream_database": ""
                                })
                                # Then do recursion for that CalculatedField
                                context = {
                                    "workbook_name": workbook_name,
                                    "sheet_name": sheet_name,
                                    "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                                    "dashboard_name": dash_name,
                                    "parent_field_name": uf_name
                                }
                                traverse_upstream_fields(uf_id, calc_field_lookup, lineage_data, context)

                            else:
                                # It's presumably a ColumnField or another DataSourceField
                                # Then we treat it like we do for columns: flatten references
                                # If it has upstreamTables, upstreamColumns, upstreamDatabases in the future
                                # it might not be in the sheetField instance. For simplicity, 
                                # let's just do the standard "flatten references" with the sheet-level data:
                                if not (upstream_tables or upstream_columns or upstream_databases):
                                    lineage_data.append({
                                        "workbook_name": workbook_name,
                                        "worksheet_name": sheet_name,
                                        "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                                        "dashboard_name": dash_name,
                                        "field_name": field_name,
                                        "field_type": "DatasourceField",
                                        "upstream_field_name": uf_name,
                                        "upstream_field_type": uf_type,
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
                                                    "upstream_field_name": uf_name,
                                                    "upstream_field_type": uf_type,
                                                    "formula": "",
                                                    "upstream_column": col["name"] if col else "",
                                                    "upstream_table": tbl["name"] if tbl else "",
                                                    "upstream_schema": tbl["schema"] if tbl else "",
                                                    "upstream_database": db["name"] if db else ""
                                                })
                    else:
                        # 2) If no upstreamFields, do the original "flatten references"
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
                    # Same as baseline: look up from calc_field_lookup
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

    # -------------------------------------------------------------------------
    # E) Build DataFrame, deduplicate, return
    # -------------------------------------------------------------------------
    df_lineage = pd.DataFrame(lineage_data)
    df_lineage.drop_duplicates(inplace=True)
    return df_lineage

# =============================================================================
# 5. MAIN
# =============================================================================
def main():
    # Read workbook names from 'workbooks.txt'
    with open('workbooks.txt', 'r') as f:
        workbook_names = [line.strip() for line in f if line.strip()]

    # Create single Excel with multiple sheets
    with pd.ExcelWriter("lineage_output.xlsx", engine="openpyxl") as writer:
        found_any_data = False
        for wb_name in workbook_names:
            print(f"\nProcessing workbook: {wb_name}")
            df_lineage = process_single_workbook(wb_name)

            if df_lineage.empty:
                print(f" - No data found or workbook '{wb_name}' not found.")
                continue

            found_any_data = True
            # Excel sheet names have 31 char limit
            safe_sheet_name = wb_name[:31] or "Sheet"
            df_lineage.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            print(f" - Wrote {len(df_lineage)} rows to sheet '{safe_sheet_name}'.")

        # If absolutely no data for any workbook, at least add a dummy sheet:
        if not found_any_data:
            df_dummy = pd.DataFrame({"No data found": []})
            df_dummy.to_excel(writer, sheet_name="EmptyResults", index=False)
            print(" - Created a dummy 'EmptyResults' sheet because no workbooks had data.")

    print("\n--- Done. Check 'lineage_output.xlsx' for results. ---")


if __name__ == "__main__":
    main()
