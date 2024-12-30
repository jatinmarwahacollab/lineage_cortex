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
#    (ADDS upstreamFields INSIDE fields)
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
#    ADDS A CASE FOR "ColumnField" => WE FLATTEN DB INFO
# =============================================================================
def traverse_upstream_fields(current_field_id, calculated_fields_dict, lineage_rows, context):
    """
    If 'current_field_id' is a CalculatedField, we expand it in a depth-first manner:
      - If the upstream is another CalculatedField, keep recursing.
      - If the upstream is a DatasourceField, record the relationship with flattening
        of upstream tables/columns/databases; then check if that DSF references
        another CalculatedField via 'upstreamFields'.
      - If the upstream is a ColumnField, we flatten its DB info in the lineage row.
      - If no upstream fields exist, record a single row as constant/no upstream.
    """
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

    cal_obj = calculated_fields_dict[current_field_id]
    cal_name = cal_obj["name"]
    cal_formula = cal_obj["formula"]
    upstreams = cal_obj.get("fields", [])

    # If no upstream => constant or no direct upstream
    if not upstreams:
        lineage_rows.append({
            "workbook_name": context["workbook_name"],
            "worksheet_name": context["sheet_name"],
            "data_source_name": context["data_source_name"],
            "dashboard_name": context["dashboard_name"],
            "field_name": context["parent_field_name"],
            "field_type": "CalculatedField",
            "upstream_field_name": cal_name,
            "upstream_field_type": "Constant/NoUpstream",
            "formula": cal_formula,
            "upstream_column": "",
            "upstream_table": "",
            "upstream_schema": "",
            "upstream_database": ""
        })
        return

    # Otherwise, iterate upstream fields
    for uf in upstreams:
        uf_id = uf["id"]
        uf_name = uf["name"]
        uf_type = uf["__typename"]
        up_tables = uf.get("upstreamTables", [])
        up_cols = uf.get("upstreamColumns", [])
        up_dbs = uf.get("upstreamDatabases", [])
        dsf_upstream_fields = uf.get("upstreamFields", []) or []

        if uf_type == "CalculatedField":
            # Calc -> Calc
            lineage_rows.append({
                "workbook_name": context["workbook_name"],
                "worksheet_name": context["sheet_name"],
                "data_source_name": context["data_source_name"],
                "dashboard_name": context["dashboard_name"],
                "field_name": context["parent_field_name"],
                "field_type": "CalculatedField",
                "upstream_field_name": uf_name,
                "upstream_field_type": "CalculatedField",
                "formula": cal_formula,
                "upstream_column": "",
                "upstream_table": "",
                "upstream_schema": "",
                "upstream_database": ""
            })
            new_ctx = context.copy()
            new_ctx["parent_field_name"] = uf_name
            traverse_upstream_fields(uf_id, calculated_fields_dict, lineage_rows, new_ctx)

        elif uf_type == "DatasourceField":
            # Calc -> DS
            # Flatten references
            if not (up_tables or up_cols or up_dbs):
                lineage_rows.append({
                    "workbook_name": context["workbook_name"],
                    "worksheet_name": context["sheet_name"],
                    "data_source_name": context["data_source_name"],
                    "dashboard_name": context["dashboard_name"],
                    "field_name": context["parent_field_name"],
                    "field_type": "CalculatedField",
                    "upstream_field_name": uf_name,
                    "upstream_field_type": "DatasourceField",
                    "formula": cal_formula,
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
                                "upstream_field_name": uf_name,
                                "upstream_field_type": "DatasourceField",
                                "formula": cal_formula,
                                "upstream_column": col["name"] if col else "",
                                "upstream_table": tbl["name"] if tbl else "",
                                "upstream_schema": "",
                                "upstream_database": db["name"] if db else ""
                            })

            # Check DSF's own upstream fields
            for dsf_up in dsf_upstream_fields:
                dsf_up_id = dsf_up["id"]
                dsf_up_type = dsf_up["__typename"]
                dsf_up_name = dsf_up["name"]

                if dsf_up_type == "CalculatedField":
                    # DSF -> sub-Calc
                    lineage_rows.append({
                        "workbook_name": context["workbook_name"],
                        "worksheet_name": context["sheet_name"],
                        "data_source_name": context["data_source_name"],
                        "dashboard_name": context["dashboard_name"],
                        "field_name": uf_name,
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
                    # DSF -> ColumnField or another DSF
                    lineage_rows.append({
                        "workbook_name": context["workbook_name"],
                        "worksheet_name": context["sheet_name"],
                        "data_source_name": context["data_source_name"],
                        "dashboard_name": context["dashboard_name"],
                        "field_name": uf_name,
                        "field_type": "DatasourceField",
                        "upstream_field_name": dsf_up_name,
                        "upstream_field_type": dsf_up_type,
                        "formula": "",
                        "upstream_column": "",
                        "upstream_table": "",
                        "upstream_schema": "",
                        "upstream_database": ""
                    })

        elif uf_type == "ColumnField":
            # NEW CASE: If we've reached a ColumnField, flatten DB-level details
            # Each column can appear in multiple tables/DB combos, so we flatten them
            if not (up_tables or up_cols or up_dbs):
                lineage_rows.append({
                    "workbook_name": context["workbook_name"],
                    "worksheet_name": context["sheet_name"],
                    "data_source_name": context["data_source_name"],
                    "dashboard_name": context["dashboard_name"],
                    "field_name": context["parent_field_name"],
                    "field_type": "CalculatedField",
                    "upstream_field_name": uf_name,
                    "upstream_field_type": "ColumnField",
                    "formula": cal_formula,
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
                                "upstream_field_name": uf_name,
                                "upstream_field_type": "ColumnField",
                                "formula": cal_formula,
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
                "upstream_field_name": uf_name,
                "upstream_field_type": uf_type,
                "formula": cal_formula,
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
                if field_type == "DatasourceField":
                    # If the sheet field references a CalcField
                    if ds_upstream_fields:
                        for uf in ds_upstream_fields:
                            uf_type = uf["__typename"]
                            uf_name = uf["name"]
                            uf_id = uf["id"]

                            if uf_type == "CalculatedField":
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
                                context = {
                                    "workbook_name": workbook_name,
                                    "sheet_name": sheet_name,
                                    "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                                    "dashboard_name": dash_name,
                                    "parent_field_name": uf_name
                                }
                                traverse_upstream_fields(uf_id, calc_field_lookup, lineage_data, context)
                            else:
                                # Probably a ColumnField or DSF 
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
                        # Original flatten if no upstreamFields
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
                    # If it's directly a CalcField
                    if field_id not in calc_field_lookup:
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
                        ctx = {
                            "workbook_name": workbook_name,
                            "sheet_name": sheet_name,
                            "data_source_name": ", ".join(ds["name"] for ds in upstream_datasources),
                            "dashboard_name": dash_name,
                            "parent_field_name": field_name
                        }
                        traverse_upstream_fields(field_id, calc_field_lookup, lineage_data, ctx)

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

    # Deduplicate
    df_lineage = pd.DataFrame(lineage_data)
    df_lineage.drop_duplicates(inplace=True)
    return df_lineage

# =============================================================================
# 5. MAIN
# =============================================================================
def main():
    with open("workbooks.txt","r") as f:
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
            dummy_df.to_excel(writer, "EmptyResults", index=False)
            print(" - Created 'EmptyResults' sheet. No data for any workbook.")

    print("\n--- Done. Check lineage_output.xlsx. ---")


if __name__ == "__main__":
    main()
