import json
import math

# Load combined_lineage data
def load_data(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Utility function to handle NaN, None, and "NA" cases
def clean_value(value):
    if value is None or value == 'NA' or value == 'NaN' or (isinstance(value, float) and math.isnan(value)):
        return None
    return value

# Create a node with a name, parent_id, table, type, and additional attributes
def create_node(node_list, name, parent_id, node_type, table=None, formula=None, column_description=None, reasoning=None):
    # If parent_id is None, replace it with 0
    if parent_id is None:
        parent_id = 0

    node_id = len(node_list) + 1
    node_list.append({
        'key': node_id,
        'name': name,
        'parent': parent_id,
        'table': clean_value(table),
        'type': node_type,
        'formula': clean_value(formula),
        'column_description': clean_value(column_description),
        'reasoning': clean_value(reasoning)
    })
    return node_id

# Recursively process database lineage
def process_database_lineage(lineage, parent_id, node_list):
    model_name = lineage['model']
    column_name = lineage['column']
    column_description = clean_value(lineage.get('column Description', None))
    reasoning = clean_value(lineage.get('reasoning', None))
    
    # Create the node for this column, type is Database
    db_node_id = create_node(
        node_list, 
        column_name,  # Store only the column name in the "name" field
        parent_id, 
        node_type="Database", 
        table=model_name,
        column_description=column_description,
        reasoning=reasoning
    )
    
    # Process upstream models recursively
    upstream_models = lineage.get('upstream_models', [])
    for upstream_model in upstream_models:
        process_database_lineage(upstream_model, db_node_id, node_list)


# Recursively process upstream fields
def handle_upstream_fields(fields, parent_id, node_list):
    """
    Processes upstreamFields recursively, handles upstreamColumns and upstreamTables.
    """
    for field in fields:
        formula = clean_value(field.get('formula', None))  # Capture formula if available
        # Create a node with type "Field"
        field_id = create_node(node_list, field['name'], parent_id, node_type="Field", formula=formula)
        
        # Process upstream columns within the field
        for column in field.get('upstreamColumns', []):
            column_table = clean_value(column.get('upstreamTables', [{}])[0].get('name'))
            column_type = "Datasource Column" if column_table else "Field"  # Determine if it's a Datasource Column or Field
            column_id = create_node(
                node_list, 
                column['name'], 
                field_id, 
                node_type=column_type, 
                table=column_table
            )
            
            # If there is database lineage, process it recursively
            if 'database_lineage' in column:
                process_database_lineage(column['database_lineage'], column_id, node_list)
        
        # Process nested upstream fields if present
        if 'upstreamFields' in field:
            handle_upstream_fields(field['upstreamFields'], field_id, node_list)

# Generate nodes for all workbooks, dashboards, etc.
def generate_nodes(workbooks):
    node_list = []
    for workbook in workbooks:
        # Create a node with type "Workbook"
        wb_id = create_node(node_list, workbook['name'], None, node_type="Workbook")
        
        for dashboard in workbook.get('dashboards', []):
            # Create a node with type "Dashboard"
            db_id = create_node(node_list, dashboard['name'], wb_id, node_type="Dashboard")
            
            for datasource in dashboard.get('upstreamDatasources', []):
                # Create a node with type "Datasource"
                ds_id = create_node(node_list, datasource['name'], db_id, node_type="Datasource")
                
                for sheet in datasource.get('sheets', []):
                    # Create a node with type "Sheet"
                    sheet_id = create_node(node_list, sheet['name'], ds_id, node_type="Sheet")
                    
                    # Handle upstream fields recursively
                    handle_upstream_fields(sheet.get('upstreamFields', []), sheet_id, node_list)
    
    return node_list

# Load data from combined_lineage.json
data = load_data('combined_lineage.json')

# Generate nodes
nodes = generate_nodes(data['workbooks'])

# Output the nodes to a file for GoJS visualization
with open('transformed_lineage.json', 'w') as f:
    json.dump(nodes, f, indent=2)

print("Transformed lineage file generated successfully.")
