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
#    (NO CHANGES TO THE LOGIC)
# =============================================================================
def traverse_upstream_fields(current_field_id, calculated_fields_dict, lineage_rows, context):
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

    # If no upstream => might be a constant/noUpstream
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

    # Otherwise, iterate
    for upstream_field in upstreams:
        up_type = upstream_field["__typename"]
        up_name = upstream_field["name"]
        up_id = upstream_field["id"]

        up_tables = upstream_field.get("upstreamTables", [])
        up_cols   = upstream_field.get("upstreamColumns", [])
        up_dbs    = upstream_field.get("upstreamDatabases", [])
        dsf_upstream_fields = upstream_field.get("upstreamFields", []) or []

        if up_type == "CalculatedField":
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
            new_ctx = context.copy()
            new_ctx["parent_field_name"] = up_name
            traverse_upstream_fields(up_id, calculated_fields_dict, lineage_rows, new_ctx)

        elif up_type == "DatasourceField":
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

            for dsf_up in dsf_upstream_fields:
                dsf_up_id   = dsf_up["id"]
                dsf_up_type = dsf_up["__typename"]
                dsf_up_name = dsf_up["name"]

                if dsf_up_type == "CalculatedField":
                    lineage_rows.append({
                        "workbook_name": context["workbook_name"],
                        "worksheet_name": context["sheet_name"],
                        "data_source_name": context["data_source_name"],
                        "dashboard_name": context["dashboard_name"],
                        "field_name": up_name,
                        "field_type": "DatasourceField",
                        "upstream_field_name": dsf_up_name,
                        "upstream_field_type": "CalculatedField",
                        "formula": "",
                        "upstream_column": "",
                        "upstream_table": "",
                        "upstream_schema": "",
                        "upstream_database": ""
                    })
                    new_ctx2 = context.copy()
                    new_ctx2["parent_field_name"] = dsf_up_name
                    traverse_upstream_fields(dsf_up_id, calculated_fields_dict, lineage_rows, new_ctx2)
                else:
                    lineage_rows.append({
                        "workbook_name": context["workbook_name"],
                        "worksheet_name": context["sheet_name"],
                        "data_source_name": context["data_source_name"],
                        "dashboard_name": context["dashboard_name"],
                        "field_name": up_name,
                        "field_type": "DatasourceField",
                        "upstream_field_name": dsf_up_name,
                        "upstream_field_type": dsf_up_type,
                        "formula": "",
                        "upstream_column": "",
                        "upstream_table": "",
                        "upstream_schema": "",
                        "upstream_database": ""
                    })

        elif up_type == "ColumnField":
            if not (up_tables or up_cols or up_dbs):
                lineage_rows.append({
                    "workbook_name": context["workbook_name"],
                    "worksheet_name": context["sheet_name"],
                    "data_source_name": context["data_source_name"],
                    "dashboard_name": context["dashboard_name"],
                    "field_name": context["parent_field_name"],
                    "field_type": "CalculatedField",
                    "upstream_field_name": up_name,
                    "upstream_field_type": "ColumnField",
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
                                "upstream_field_type": "ColumnField",
                                "formula": cal_field_formula,
                                "upstream_column": col["name"] if col else "",
                                "upstream_table": tbl["name"] if tbl else "",
                                "upstream_schema": "",
                                "upstream_database": db["name"] if db else ""
                            })

        else:
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
# 4. PROCESS SINGLE WORKBOOK (ADD 'primary_field' FOR SHEET FIELD INSTANCES ONLY)
# =============================================================================
def process_single_workbook(wb_name):
    wb_json_data = get_workbook_details(wb_name)
    workbooks_data = wb_json_data.get("data", {}).get("workbooks", [])
    if not workbooks_data:
        print(f"No workbook found with name '{wb_name}'. Skipping.")
        return pd.DataFrame()

    workbook_obj = workbooks_data[0]
    workbook_name = workbook_obj["name"]

    # Gather CalcField IDs from dashboards & sheet fields
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

    # Fetch calc fields in batch
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

    # Build lineage
    lineage_data = []
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

            # For each referencing dashboard, produce rows
            for dash_name in dash_names:
                # We add "primary_field": True ONLY for these sheet-level rows
                # (the recursion remains unchanged and won't have this key).
                primary_row_base = {
                    "workbook_name": workbook_name,
                    "worksheet_name": sheet_name,
                    "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                    "dashboard_name": dash_name,
                    "field_name": field_name,
                    "field_type": field_type,
                    "primary_field": True  # <-- NEW
                }

                if field_type == "DatasourceField":
                    if ds_upstream_fields:
                        for uf in ds_upstream_fields:
                            uf_type = uf["__typename"]
                            uf_name = uf["name"]
                            uf_id = uf["id"]

                            row = primary_row_base.copy()
                            row.update({
                                "upstream_field_name": uf_name,
                                "upstream_field_type": uf_type,
                                "formula": "",
                                "upstream_column": "",
                                "upstream_table": "",
                                "upstream_schema": "",
                                "upstream_database": ""
                            })

                            if uf_type == "CalculatedField":
                                # Fill formula if we have it
                                row["formula"] = calc_field_lookup.get(uf_id, {}).get("formula", "")
                                lineage_data.append(row)

                                # Then do recursion
                                context = {
                                    "workbook_name": workbook_name,
                                    "sheet_name": sheet_name,
                                    "data_source_name": row["data_source_name"],
                                    "dashboard_name": dash_name,
                                    "parent_field_name": uf_name
                                }
                                traverse_upstream_fields(uf_id, calc_field_lookup, lineage_data, context)
                            else:
                                # Possibly a ColumnField or DSF, flatten references if needed
                                if not (upstream_tables or upstream_columns or upstream_databases):
                                    lineage_data.append(row)
                                else:
                                    for tbl in (upstream_tables or [None]):
                                        for col in (upstream_columns or [None]):
                                            for db in (upstream_databases or [None]):
                                                row2 = row.copy()
                                                row2.update({
                                                    "upstream_column": col["name"] if col else "",
                                                    "upstream_table": tbl["name"] if tbl else "",
                                                    "upstream_schema": tbl["schema"] if tbl else "",
                                                    "upstream_database": db["name"] if db else ""
                                                })
                                                lineage_data.append(row2)
                    else:
                        # Flatten if no upstream fields
                        row = primary_row_base.copy()
                        row.update({
                            "upstream_field_name": "",
                            "upstream_field_type": "",
                            "formula": "",
                            "upstream_column": "",
                            "upstream_table": "",
                            "upstream_schema": "",
                            "upstream_database": ""
                        })
                        if not (upstream_tables or upstream_columns or upstream_databases):
                            lineage_data.append(row)
                        else:
                            for tbl in (upstream_tables or [None]):
                                for col in (upstream_columns or [None]):
                                    for db in (upstream_databases or [None]):
                                        row2 = row.copy()
                                        row2.update({
                                            "upstream_column": col["name"] if col else "",
                                            "upstream_table": tbl["name"] if tbl else "",
                                            "upstream_schema": tbl["schema"] if tbl else "",
                                            "upstream_database": db["name"] if db else ""
                                        })
                                        lineage_data.append(row2)

                elif field_type == "CalculatedField":
                    if field_id not in calc_field_lookup:
                        # Possibly unknown
                        row = primary_row_base.copy()
                        row.update({
                            "upstream_field_name": "UNKNOWN",
                            "upstream_field_type": "UNKNOWN",
                            "formula": "",
                            "upstream_column": "",
                            "upstream_table": "",
                            "upstream_schema": "",
                            "upstream_database": ""
                        })
                        lineage_data.append(row)
                    else:
                        # We can produce one row for the top-level calc
                        row = primary_row_base.copy()
                        row.update({
                            "upstream_field_name": calc_field_lookup[field_id]["name"],
                            "upstream_field_type": "CalculatedField",
                            "formula": calc_field_lookup[field_id]["formula"],
                            "upstream_column": "",
                            "upstream_table": "",
                            "upstream_schema": "",
                            "upstream_database": ""
                        })
                        lineage_data.append(row)

                        # Then do recursion
                        ctx = {
                            "workbook_name": workbook_name,
                            "sheet_name": sheet_name,
                            "data_source_name": row["data_source_name"],
                            "dashboard_name": dash_name,
                            "parent_field_name": calc_field_lookup[field_id]["name"]
                        }
                        traverse_upstream_fields(field_id, calc_field_lookup, lineage_data, ctx)

                else:
                    # Some other type
                    row = primary_row_base.copy()
                    row.update({
                        "upstream_field_name": "",
                        "upstream_field_type": "",
                        "formula": "",
                        "upstream_column": "",
                        "upstream_table": "",
                        "upstream_schema": "",
                        "upstream_database": ""
                    })
                    lineage_data.append(row)

    df_lineage = pd.DataFrame(lineage_data)
    df_lineage.drop_duplicates(inplace=True)
    return df_lineage

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
