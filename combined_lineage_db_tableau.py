import json

# Load Tableau lineage
with open('tableau_lineage.json', 'r') as f:
    tableau_data = json.load(f)

# Load Database lineage
with open('lineage.json', 'r') as f:
    db_lineage_data = json.load(f)

# Helper function to recursively find matching column and table in database lineage
def find_matching_db_lineage(tableau_column, tableau_table, db_lineage):
    """
    Finds a matching column in the db_lineage based on both column and table name.
    Recursively searches upstream models if no direct match is found.
    """
    for db_entry in db_lineage:
        if tableau_column.lower() == db_entry["column"].lower() and tableau_table.lower() == db_entry["model"].lower():
            return db_entry
        
        # If there are upstream models, recursively search them
        upstream_models = db_entry.get("upstream_models", [])
        if upstream_models:
            matching_upstream = find_matching_db_lineage(tableau_column, tableau_table, upstream_models)
            if matching_upstream:
                return matching_upstream
    
    return None

# Recursive function to process upstream fields and match to database lineage
def process_upstream_fields(upstream_fields, db_lineage_data, context=""):
    """
    Processes upstream fields recursively, comparing upstream columns and tables with database lineage.
    Handles nested upstreamTables inside upstreamColumns.
    """
    for upstream_field in upstream_fields:
        upstream_columns = upstream_field.get("upstreamColumns", [])
        
        # For each upstream column, check for nested upstreamTables
        for upstream_column in upstream_columns:
            upstream_tables = upstream_column.get("upstreamTables", [])  # Now nested inside upstreamColumns
            
            for upstream_table in upstream_tables:
                # Find matching database lineage using both the column and table
                matching_db_lineage = find_matching_db_lineage(upstream_column["name"], upstream_table["name"], db_lineage_data)
                if matching_db_lineage:
                    # Add the matched DB lineage details to the Tableau upstream column
                    upstream_column["database_lineage"] = matching_db_lineage
                    print(f"DEBUG: Attached DB lineage for column: {upstream_column['name']} in table: {upstream_table['name']} within {context}")
                
                # Log a message if no matching DB lineage is found
                else:
                    print(f"WARNING: No matching DB lineage found for column: {upstream_column['name']} in table: {upstream_table['name']} within {context}")
        
        # Recursively process nested upstreamFields if present
        nested_upstream_fields = upstream_field.get("upstreamFields", [])
        if nested_upstream_fields:
            process_upstream_fields(nested_upstream_fields, db_lineage_data, context=f"{context} -> Nested Field")

# Function to process non-calculated columns
def process_non_calculated_fields(datasource, db_lineage_data):
    """
    Handles fields that are not part of calculations and ensures their upstream lineage is correctly processed.
    """
    for sheet in datasource["sheets"]:
        for upstream_field in sheet["upstreamFields"]:
            process_upstream_fields([upstream_field], db_lineage_data, context=f"Sheet: {sheet['name']}")

# Function to merge database lineage into Tableau lineage
def merge_lineage(tableau_data, db_lineage_data):
    """
    Iterates over the Tableau lineage and matches it with the database lineage.
    """
    for workbook in tableau_data["workbooks"]:
        for dashboard in workbook["dashboards"]:
            for datasource in dashboard["upstreamDatasources"]:
                # Process non-calculated fields
                process_non_calculated_fields(datasource, db_lineage_data)

                # Process referencedByCalculations if they exist
                for sheet in datasource["sheets"]:
                    for upstream_field in sheet["upstreamFields"]:
                        if "referencedByCalculations" in upstream_field:
                            for calc in upstream_field["referencedByCalculations"]:
                                # Process upstreamFields within referenced calculations
                                process_upstream_fields(calc.get("upstreamFields", []), db_lineage_data, context=f"Calculation in Sheet: {sheet['name']}")

    return tableau_data

# Merge the lineages
combined_lineage = merge_lineage(tableau_data, db_lineage_data)

# Output the merged lineage to a file
with open('combined_lineage.json', 'w') as f:
    json.dump(combined_lineage, f, indent=4)

print("Merged lineage file generated successfully.")
