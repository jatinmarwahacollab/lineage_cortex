import json
import requests

# Replace these with your actual Tableau Online details
instance = "prod-apnortheast-a"
api_version = "3.14"
auth_url = f"https://{instance}.online.tableau.com/api/{api_version}/auth/signin"

token_name = "demo_lineage"
token_value = "OduNru8eTcevWyUj75fFHQ==:NwB6cBGwWeOjrhSbVoUkIIFLdxy67ACh"
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

# Authenticate
try:
    response = requests.post(auth_url, json=auth_payload, headers=auth_headers)
    response.raise_for_status()
    data = response.json()
    auth_token = data['credentials']['token']
    print(f"Authenticated with token: {auth_token}")
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
    exit()

# Define the GraphQL endpoint
metadata_api_url = f"https://{instance}.online.tableau.com/api/metadata/graphql"
headers = {
    "Content-Type": "application/json",
    "X-Tableau-Auth": auth_token
}

# Step 1: Fetch the list of IDs for the published datasource
fetch_datasource_query = """
{
  publishedDatasources {
    id
    name
  }
}
"""

response = requests.post(metadata_api_url, json={'query': fetch_datasource_query}, headers=headers)
response.raise_for_status()

# Parse the JSON response to get the datasource IDs
datasource_data = response.json()

# Extract multiple IDs from the response
published_datasource_ids = [ds['id'] for ds in datasource_data['data']['publishedDatasources']]

print(f"Published Datasource IDs: {published_datasource_ids}")

# Prepare the list of IDs for the `idWithin` filter
idWithin_str = '", "'.join(published_datasource_ids)
idWithin_filter = f'["{idWithin_str}"]'

# Step 2: Construct the main GraphQL query using sheetFieldInstances and upstreamFields
graphql_query = f"""
{{
  workbooks(filter: {{name: "Jaffle Shop "}}) {{
    name
    dashboard: dashboards(filter: {{name: "Dashboard 1"}}) {{
      name
      id
      upstreamDatasources(filter: {{idWithin: {idWithin_filter}}}) {{
        name
        downstreamSheets {{
          name
          worksheetFields {{
            name
          }}
          sheetFieldInstances(orderBy: {{field: NAME, direction: ASC}}) {{
            upstreamFields {{
              name
              upstreamDatabases {{
                name
              }}
              upstreamTables {{
                name
              }}
              upstreamColumns {{
                name
              }}
              referencedByCalculations {{
                name
                formula
                upstreamFields {{
                  name
                  upstreamDatabases {{
                    name
                  }}
                  upstreamTables {{
                    name
                  }}
                  upstreamColumns {{
                    name
                  }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# Make the request to the GraphQL endpoint (use updated headers with auth token)
response = requests.post(metadata_api_url, json={'query': graphql_query}, headers=headers)
response.raise_for_status()

# Parse the JSON response
data = response.json()
print(json.dumps(data, indent=2))

def deduplicate_fields(fields):
    """ Function to remove duplicate upstream fields based on the 'name' key """
    seen = set()
    unique_fields = []
    for field in fields:
        field_name = field["name"]
        if field_name not in seen:
            unique_fields.append(field)
            seen.add(field_name)
    return unique_fields

def build_lineage(data):
    output = {"workbooks": []}

    # Iterate over the workbooks
    for workbook in data['data']['workbooks']:
        workbook_output = {
            "name": workbook["name"],
            "dashboards": []
        }

        # Iterate over the dashboards in the workbook
        for dashboard in workbook["dashboard"]:
            dashboard_output = {
                "name": dashboard["name"],
                "upstreamDatasources": []
            }

            # Iterate over the upstreamDatasources in the dashboard
            for datasource in dashboard["upstreamDatasources"]:
                datasource_output = {
                    "name": datasource["name"],
                    "sheets": []
                }

                # Iterate over the downstreamSheets under each datasource
                for sheet in datasource["downstreamSheets"]:
                    sheet_output = {
                        "name": sheet["name"],
                        "worksheetFields": [],
                        "upstreamFields": []
                    }

                    # Add worksheetFields to sheet output
                    for field in sheet["worksheetFields"]:
                        worksheet_field_output = {
                            "name": field["name"]
                        }
                        sheet_output["worksheetFields"].append(worksheet_field_output)

                    # Add sheetFieldInstances and process upstreamFields
                    for sheet_field_instance in sheet["sheetFieldInstances"]:
                        for upstream_field in sheet_field_instance["upstreamFields"]:
                            field_output = {
                                "name": upstream_field["name"],
                                "upstreamColumns": [],
                                "formula": ""
                            }

                            # Add direct upstream details for each column in the field
                            for column in upstream_field.get("upstreamColumns", []):
                                column_entry = {
                                    "name": column["name"],
                                    "upstreamDatabases": upstream_field.get("upstreamDatabases", []),
                                    "upstreamTables": upstream_field.get("upstreamTables", [])
                                }
                                field_output["upstreamColumns"].append(column_entry)

                            # Handle referencedByCalculations first, with new hierarchy of upstreamFields
                            process_calculations_with_upstream_fields(upstream_field, sheet_output)

                            # Add the upstream field to the sheet output
                            sheet_output["upstreamFields"].append(field_output)

                    # Deduplicate upstream fields after processing
                    sheet_output["upstreamFields"] = deduplicate_fields(sheet_output["upstreamFields"])

                    # Add sheet output to the datasource
                    datasource_output["sheets"].append(sheet_output)

                # Add datasource output to the dashboard
                dashboard_output["upstreamDatasources"].append(datasource_output)

            # Add dashboard output to the workbook
            workbook_output["dashboards"].append(dashboard_output)

        # Add the workbook output to the final result
        output["workbooks"].append(workbook_output)

    return output

# Function to handle recursive referencedByCalculations with deduplication and new upstreamFields hierarchy
def process_calculations_with_upstream_fields(upstream_field, sheet_output):
    if upstream_field.get("referencedByCalculations"):
        for calc in upstream_field["referencedByCalculations"]:
            calc_entry = {
                "name": calc["name"],
                "formula": calc.get("formula", ""),
                "upstreamFields": []
            }

            # Add upstreamFields for the referenced calculation
            for calc_upstream_field in calc.get("upstreamFields", []):
                calc_field_entry = {
                    "name": calc_upstream_field["name"],
                    "upstreamColumns": [],
                    "upstreamDatabases": calc_upstream_field.get("upstreamDatabases", []),
                    "upstreamTables": calc_upstream_field.get("upstreamTables", [])
                }

                # Add upstream columns for the referenced calculation's upstream fields
                for upstream_col in calc_upstream_field.get("upstreamColumns", []):
                    upstream_column_entry = {
                        "name": upstream_col["name"],
                        "upstreamDatabases": calc_upstream_field.get("upstreamDatabases", []),
                        "upstreamTables": calc_upstream_field.get("upstreamTables", [])
                    }
                    calc_field_entry["upstreamColumns"].append(upstream_column_entry)

                # Append each upstreamField inside the referenced calculation
                calc_entry["upstreamFields"].append(calc_field_entry)

            # Treat each referenced calculation as a separate entry
            sheet_output["upstreamFields"].append(calc_entry)

# Generate the output
lineage_output = build_lineage(data)

# Write the output to a file to review
with open('tableau_lineage.json', 'w') as f:
    json.dump(lineage_output, f, indent=4)

print("Lineage file generated successfully.")
