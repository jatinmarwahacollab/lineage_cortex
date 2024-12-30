import json
import requests
import pandas as pd

# =============================================================================
# REPLACE THESE WITH YOUR TABLEAU ONLINE DETAILS
# =============================================================================
instance = "prod-apnortheast-a"
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
# METADATA API ENDPOINT & HEADERS
# =============================================================================
metadata_api_url = f"https://{instance}.online.tableau.com/api/metadata/graphql"
headers = {
    "Content-Type": "application/json",
    "X-Tableau-Auth": auth_token
}

# =============================================================================
# 1. GRAPHQL QUERY (ENHANCED FOR upstreamFields IN SHEET FIELDS)
# =============================================================================
def get_workbook_details(workbook_name):
    """
    Returns JSON data for the specified workbook name, including dashboards,
    sheet fields, upstream columns, plus 'upstreamFields' for each sheet field
    so that we can detect if a DatasourceField is actually referencing a
    CalculatedField or a ColumnField behind the scenes.
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
    """
    Returns JSON data for CalculatedFields, including their formula and each
    upstream field (which may be another CalculatedField or a DatasourceField).
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
# 3. RECURSIVE TRAVERSAL FOR CALCULATED FIELD DEPENDENCIES
# =============================================================================
def traverse_upstream_fields(current_field_id, calculated_fields_dict, lineage_rows, context):
    """
    If 'current_field_id' is a CalculatedField, we expand it in a depth-first manner:
    - If the upstream is another CalculatedField, keep recursing
    - If the upstream is a DatasourceField, record the relationship with flattening
      of upstream tables/columns/databases
    - If no upstream fields exist, record a single row marking it as constant/no upstream
    """
    # If we don't have current_field_id in dict => UNKNOWN
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

    # If no upstream => constant or no direct upstream
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

    # Otherwise, iterate over upstream fields
    for upstream_field in upstreams:
        up_type = upstream_field["__typename"]
        up_name = upstream_field["name"]
        up_id = upstream_field["id"]

        if up_type == "CalculatedField":
            # 1) Record direct relationship: parent -> child calc
            lineage_rows.append({
                "workbook_name": context["workbook_name"],
                "worksheet_name": context["sheet_name"],
                "data_source_name": context["data_source_name"],
                "dashboard_name": context["dashboard_name"],
                "field_name": context["parent_field_name"],
                "field_type": "CalculatedField",
                "upstream_field_name": up_name,
                "upstream_field_type": "CalculatedField",
                "formula": cal_field_formula,
                "upstream_column": "",
                "upstream_table": "",
                "upstream_schema": "",
                "upstream_database": ""
            })
            # 2) Recurse further
            new_context = context.copy()
            new_context["parent_field_name"] = up_name
            traverse_upstream_fields(up_id, calculated_fields_dict, lineage_rows, new_context)

        elif up_type == "DatasourceField":
            # Flatten references
            up_tables = upstream_field.get("upstreamTables", [])
            up_cols = upstream_field.get("upstreamColumns", [])
            up_dbs = upstream_field.get("upstreamDatabases", [])

            if not (up_tables or up_cols or up_dbs):
                lineage_rows.append({
                    "workbook_name": context["workbook_name"],
                    "worksheet_name": context["sheet_name"],
                    "data_source_name": context["data_source_name"],
                    "dashboard_name": context["dashboard_name"],
                    "field_name": context["parent_field_name"],
                    "field_type": "CalculatedField",
                    "upstream_field_name": up_name,
                    "upstream_field_type": "DatasourceField",
                    "formula": cal_field_formula,
                    "upstream_column": "",
                    "upstream_table": "",
                    "upstream_schema": "",
                    "upstream_database": ""
                })
            else:
                for tbl in (up_tables or [None]):
                    for col in (up_cols or [None]):
                        for db in (up_dbs or [None]):
                            lineage_rows.append({
                                "workbook_name": context["workbook_name"],
                                "worksheet_name": context["sheet_name"],
                                "data_source_name": context["data_source_name"],
                                "dashboard_name": context["dashboard_name"],
                                "field_name": context["parent_field_name"],
                                "field_type": "CalculatedField",
                                "upstream_field_name": up_name,
                                "upstream_field_type": "DatasourceField",
                                "formula": cal_field_formula,
                                "upstream_column": col["name"] if col else "",
                                "upstream_table": tbl["name"] if tbl else "",
                                "upstream_schema": "",
                                "upstream_database": db["name"] if db else ""
                            })
        else:
            # Some other type
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
                "upstream_column": "",
                "upstream_table": "",
                "upstream_schema": "",
                "upstream_database": ""
            })


# =============================================================================
# 4. PROCESS SINGLE WORKBOOK
# =============================================================================
def process_single_workbook(wb_name):
    """
    1) Get workbook details
    2) Collect CalculatedField IDs from:
       - Dashboards (upstreamFields)
       - Any DatasourceField that might reference a CalculatedField
    3) Batch fetch all those CalcFields
    4) Build final lineage rows for each sheet field (DatasourceField or CalculatedField)
    5) Return a DataFrame
    """
    # --- Fetch workbook details (including upstreamFields for sheet fields) ---
    wb_json_data = get_workbook_details(wb_name)
    workbooks_data = wb_json_data.get("data", {}).get("workbooks", [])
    if not workbooks_data:
        print(f"No workbook found with name '{wb_name}'. Skipping.")
        return pd.DataFrame()

    workbook_obj = workbooks_data[0]
    workbook_name = workbook_obj["name"]

    # A) Collect CalcField IDs from dashboards
    dashboards = workbook_obj.get("dashboards", [])
    calc_field_ids = []
    for dash in dashboards:
        for field in dash.get("upstreamFields", []):
            if field["__typename"] == "CalculatedField":
                calc_field_ids.append(field["id"])

    # B) Also gather CalcField IDs from any sheetFieldInstance that is a
    #    DataSourceField referencing a CalculatedField in 'upstreamFields'
    sheets = workbook_obj.get("sheets", [])
    for sheet in sheets:
        for sf in sheet.get("sheetFieldInstances", []):
            if sf["__typename"] == "DatasourceField":
                # If it references a CalculatedField
                for uf in sf.get("upstreamFields", []):
                    if uf["__typename"] == "CalculatedField":
                        calc_field_ids.append(uf["id"])

    calc_field_ids = list(set(calc_field_ids))

    # C) Batch fetch all CalcFields
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

    # D) Build lineage rows
    lineage_data = []
    for sheet in sheets:
        sheet_name = sheet["name"]
        contained_dashboards = sheet.get("containedInDashboards", [])
        dash_names = [d["name"] for d in contained_dashboards] or ["NoDashboard"]

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

                # 1) If it's a DatasourceField
                if field_type == "DatasourceField":
                    # If the sheet field itself references a CalculatedField upstream
                    # => produce a row and call 'traverse_upstream_fields'
                    if ds_upstream_fields:
                        # For each upstream
                        for uf in ds_upstream_fields:
                            uf_type = uf["__typename"]
                            uf_name = uf["name"]
                            uf_id = uf["id"]

                            if uf_type == "CalculatedField":
                                # Immediate link: DSField -> CalcField
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
                                # Recurse
                                context = {
                                    "workbook_name": workbook_name,
                                    "sheet_name": sheet_name,
                                    "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                                    "dashboard_name": dash_name,
                                    "parent_field_name": uf_name
                                }
                                traverse_upstream_fields(uf_id, calc_field_lookup, lineage_data, context)

                            else:
                                # Probably a ColumnField or another DSField
                                # => Flatten physical references from the sheet instance
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
                        # If no upstreamFields, we do the original flatten
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

                # 2) If it's a CalculatedField
                elif field_type == "CalculatedField":
                    # Same baseline approach
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

                # 3) Otherwise, treat it as an unrecognized field type
                else:
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

    # E) Build DataFrame, deduplicate, return
    df_lineage = pd.DataFrame(lineage_data)
    df_lineage.drop_duplicates(inplace=True)
    return df_lineage


# =============================================================================
# 5. MAIN
# =============================================================================
def main():
    # Read workbook names from a file
    with open("workbooks.txt", "r") as f:
        workbook_names = [line.strip() for line in f if line.strip()]

    with pd.ExcelWriter("lineage_output.xlsx", engine="openpyxl") as writer:
        wrote_any_data = False
        for wb_name in workbook_names:
            print(f"\nProcessing workbook: {wb_name}")
            df_lineage = process_single_workbook(wb_name)
            if df_lineage.empty:
                print(f" - No data found for '{wb_name}', or workbook not found.")
                continue

            # If we have data, write a sheet
            wrote_any_data = True
            safe_sheet_name = wb_name[:31] or "Sheet"
            df_lineage.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            print(f" - Wrote {len(df_lineage)} rows to sheet '{safe_sheet_name}'.")

        # If no data at all, create a dummy sheet
        if not wrote_any_data:
            dummy_df = pd.DataFrame({"No data found": []})
            dummy_df.to_excel(writer, sheet_name="EmptyResults", index=False)
            print(" - Created 'EmptyResults' sheet (no data for any workbook).")

    print("\n--- Done. Check 'lineage_output.xlsx'. ---")


if __name__ == "__main__":
    main()
