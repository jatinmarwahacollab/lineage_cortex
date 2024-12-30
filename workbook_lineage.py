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
# 1. ENHANCED SHEET FIELDS QUERY
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
    resp = requests.post(metadata_api_url, json={'query': query}, headers=headers)
    resp.raise_for_status()
    return resp.json()

# =============================================================================
# 2. BATCH FETCH CALCULATEDFIELDS - INCLUDES 'upstreamFields' 
# =============================================================================
def get_calculated_field_details(field_ids):
    if not field_ids:
        return {}
    id_within_str = '", "'.join(field_ids)
    id_within_filter = f'["{id_within_str}"]'
    query = f"""
    {{
      calculatedFields(filter: {{idWithin: {id_within_filter}}}) {{
        id
        name
        formula
        fields {{
          id
          name
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
    resp = requests.post(metadata_api_url, json={'query': query}, headers=headers)
    resp.raise_for_status()
    return resp.json()


# =============================================================================
# 3. RECURSIVE TRAVERSAL FOR CALCULATED FIELD DEPENDENCIES (MODIFIED)
# =============================================================================
def traverse_upstream_fields(current_field_id, calculated_fields_dict, lineage_rows, context):
    """
    Depth-first expansion of a CalculatedField's upstream references:
      - If upstream is another CalculatedField, we recurse
      - If upstream is a DatasourceField, we flatten references, then see if that DSF references
        a ColumnField or another CalculatedField
      - If upstream is ColumnField, we produce a single row with the parent's formula
        and the physical DB/column info
      - If no upstream fields, it's a constant
    """
    if current_field_id not in calculated_fields_dict:
        # Unknown calc
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
    cal_name = cal_field_data["name"]
    cal_formula = cal_field_data["formula"]
    upstreams = cal_field_data.get("fields", [])

    # If no upstream => constant
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
        # Physical references
        uf_tables = uf.get("upstreamTables", [])
        uf_cols = uf.get("upstreamColumns", [])
        uf_dbs = uf.get("upstreamDatabases", [])
        dsf_upstream_fields = uf.get("upstreamFields", []) or []

        if uf_type == "CalculatedField":
            # record a row parent->child calc
            lineage_rows.append({
                "workbook_name": context["workbook_name"],
                "worksheet_name": context["sheet_name"],
                "data_source_name": context["data_source_name"],
                "dashboard_name": context["dashboard_name"],
                "field_name": context["parent_field_name"],
                "field_type": "CalculatedField",
                "upstream_field_name": uf_name,
                "upstream_field_type": "CalculatedField",
                "formula": cal_formula,  # parent's formula
                "upstream_column": "",
                "upstream_table": "",
                "upstream_schema": "",
                "upstream_database": ""
            })

            # Recurse
            new_ctx = context.copy()
            new_ctx["parent_field_name"] = uf_name
            traverse_upstream_fields(uf_id, calculated_fields_dict, lineage_rows, new_ctx)

        elif uf_type == "DatasourceField":
            # Flatten DS references (calc -> DS)
            if not (uf_tables or uf_cols or uf_dbs):
                # no physical references
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
                # flatten combos
                for tbl in (uf_tables or [None]):
                    for col in (uf_cols or [None]):
                        for db in (uf_dbs or [None]):
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

            # Now see if that DSF references a ColumnField or another Calc
            for dsf_up in dsf_upstream_fields:
                dsf_up_id = dsf_up["id"]
                dsf_up_name = dsf_up["name"]
                dsf_up_type = dsf_up["__typename"]

                # If the DSF references a ColumnField, we skip a separate DS->Column row
                # and produce a direct row from the *parent* Calc to that column
                if dsf_up_type == "ColumnField":
                    # We might also flatten that column's own references if needed
                    # but typically, the column's upstreamTables, etc. are in "uf_tables" above.
                    # In some cases, you'd gather it from dsf_up if the GraphQL returned them. 
                    lineage_rows.append({
                        "workbook_name": context["workbook_name"],
                        "worksheet_name": context["sheet_name"],
                        "data_source_name": context["data_source_name"],
                        "dashboard_name": context["dashboard_name"],
                        "field_name": context["parent_field_name"],
                        "field_type": "CalculatedField",
                        "upstream_field_name": dsf_up_name,
                        "upstream_field_type": "ColumnField",
                        "formula": cal_formula,
                        "upstream_column": ", ".join(c["name"] for c in uf_cols),
                        "upstream_table": ", ".join(t["name"] for t in uf_tables),
                        "upstream_schema": "",
                        "upstream_database": ", ".join(d["name"] for d in uf_dbs)
                    })

                elif dsf_up_type == "CalculatedField":
                    # DSF -> Calc
                    lineage_rows.append({
                        "workbook_name": context["workbook_name"],
                        "worksheet_name": context["sheet_name"],
                        "data_source_name": context["data_source_name"],
                        "dashboard_name": context["dashboard_name"],
                        "field_name": uf_name,  # the DSF
                        "field_type": "DatasourceField",
                        "upstream_field_name": dsf_up_name,
                        "upstream_field_type": "CalculatedField",
                        "formula": "",
                        "upstream_column": "",
                        "upstream_table": "",
                        "upstream_schema": "",
                        "upstream_database": ""
                    })
                    # Then we call traverse for that sub-calc
                    new_ctx = context.copy()
                    new_ctx["parent_field_name"] = dsf_up_name
                    traverse_upstream_fields(dsf_up_id, calculated_fields_dict, lineage_rows, new_ctx)
                else:
                    # Possibly another DSF or something else
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
            # direct calc->column link
            # flatten references
            for tbl in (uf_tables or [None]):
                for col in (uf_cols or [None]):
                    for db in (uf_dbs or [None]):
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
            # Something else
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
# 4. PROCESS SINGLE WORKBOOK (BASELINE LOGIC + CHANGES)
# =============================================================================
def process_single_workbook(wb_name):
    wb_json_data = get_workbook_details(wb_name)
    wb_data = wb_json_data.get("data", {}).get("workbooks", [])
    if not wb_data:
        print(f"No workbook found with name '{wb_name}'. Skipping.")
        return pd.DataFrame()

    workbook_obj = wb_data[0]
    workbook_name = workbook_obj["name"]

    # Collect CalcField IDs
    dashboards = workbook_obj.get("dashboards", [])
    calc_field_ids = []
    for dash in dashboards:
        for field in dash.get("upstreamFields", []):
            if field["__typename"] == "CalculatedField":
                calc_field_ids.append(field["id"])

    # Also gather IDs from sheet fields referencing CalcFields
    sheets = workbook_obj.get("sheets", [])
    for sheet in sheets:
        for sf in sheet.get("sheetFieldInstances", []):
            if sf["__typename"] == "DatasourceField":
                for uf in sf.get("upstreamFields", []):
                    if uf["__typename"] == "CalculatedField":
                        calc_field_ids.append(uf["id"])

    calc_field_ids = list(set(calc_field_ids))

    # Batch fetch calcfields
    calc_resp = get_calculated_field_details(calc_field_ids)
    calc_fields_data = calc_resp.get("data", {}).get("calculatedFields", [])
    calc_field_lookup = {}
    for cfd in calc_fields_data:
        cfd_id = cfd["id"]
        calc_field_lookup[cfd_id] = {
            "name": cfd["name"],
            "formula": cfd["formula"],
            "fields": []
        }
        for fobj in cfd.get("fields", []):
            calc_field_lookup[cfd_id]["fields"].append({
                "id": fobj["id"],
                "name": fobj["name"],
                "__typename": fobj["__typename"],
                "upstreamTables": fobj.get("upstreamTables", []),
                "upstreamColumns": fobj.get("upstreamColumns", []),
                "upstreamDatabases": fobj.get("upstreamDatabases", []),
                "upstreamFields": fobj.get("upstreamFields", []) or []
            })

    # Build lineage
    lineage_data = []
    for sheet in sheets:
        sheet_name = sheet["name"]
        dash_names = [d["name"] for d in sheet.get("containedInDashboards", [])] or ["NoDashboard"]

        sfis = sheet.get("sheetFieldInstances", [])
        for sf in sfis:
            field_type = sf["__typename"]
            field_id = sf["id"]
            field_name = sf["name"]

            upstream_datasources = sf.get("upstreamDatasources", [])
            upstream_tables = sf.get("upstreamTables", [])
            upstream_columns = sf.get("upstreamColumns", [])
            upstream_databases = sf.get("upstreamDatabases", [])
            ds_upstream_fields = sf.get("upstreamFields", []) or []

            for dash_name in dash_names:
                if field_type == "DatasourceField":
                    if ds_upstream_fields:
                        # Check if referencing a Calc or Column
                        for uf in ds_upstream_fields:
                            uf_type = uf["__typename"]
                            uf_name = uf["name"]
                            uf_id = uf["id"]

                            if uf_type == "CalculatedField":
                                lineage_data.append({
                                    "workbook_name": workbook_name,
                                    "worksheet_name": sheet_name,
                                    "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
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
                                    "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
                                    "dashboard_name": dash_name,
                                    "parent_field_name": uf_name
                                }
                                traverse_upstream_fields(uf_id, calc_field_lookup, lineage_data, context)
                            else:
                                # Possibly a column or DSF 
                                # Flatten references from the sheet
                                if not (upstream_tables or upstream_columns or upstream_databases):
                                    lineage_data.append({
                                        "workbook_name": workbook_name,
                                        "worksheet_name": sheet_name,
                                        "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
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
                                                    "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
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
                        # No upstream fields, flatten references
                        if not (upstream_tables or upstream_columns or upstream_databases):
                            lineage_data.append({
                                "workbook_name": workbook_name,
                                "worksheet_name": sheet_name,
                                "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
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
                                            "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
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
                    if field_id not in calc_field_lookup:
                        lineage_data.append({
                            "workbook_name": workbook_name,
                            "worksheet_name": sheet_name,
                            "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
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
                            "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
                            "dashboard_name": dash_name,
                            "parent_field_name": field_name
                        }
                        traverse_upstream_fields(field_id, calc_field_lookup, lineage_data, ctx)

                else:
                    # unrecognized
                    lineage_data.append({
                        "workbook_name": workbook_name,
                        "worksheet_name": sheet_name,
                        "data_source_name": ", ".join(x["name"] for x in upstream_datasources),
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
    if not df_lineage.empty:
        df_lineage.drop_duplicates(inplace=True)
    return df_lineage


# =============================================================================
# 5. MAIN
# =============================================================================
def main():
    with open("workbooks.txt","r") as f:
        workbook_names = [line.strip() for line in f if line.strip()]

    with pd.ExcelWriter("lineage_output.xlsx", engine="openpyxl") as writer:
        wrote_data = False
        for wb_name in workbook_names:
            print(f"\nProcessing workbook: {wb_name}")
            df_lineage = process_single_workbook(wb_name)
            if df_lineage.empty:
                print(f" - No data found or workbook not found: {wb_name}")
                continue

            wrote_data = True
            safe_sheet_name = wb_name[:31] or "Sheet"
            df_lineage.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            print(f" - Wrote {len(df_lineage)} rows to '{safe_sheet_name}'.")

        if not wrote_data:
            dummy_df = pd.DataFrame({"No data found": []})
            dummy_df.to_excel(writer, "EmptyResults", index=False)
            print(" - Created 'EmptyResults' sheet. No data for any workbook.")

    print("\n--- Done. Check lineage_output.xlsx. ---")


if __name__ == "__main__":
    main()
