import pandas as pd
import snowflake.connector
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to Snowflake and read data
def read_data_from_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv('user'),
        password=os.getenv('password'),
        account=os.getenv('account'),
        warehouse=os.getenv('warehouse'),
        database=os.getenv('database'),
        schema=os.getenv('schema'),
        role=os.getenv('role')
    )

    # Query the Snowflake table
    query = """
    SELECT
        TABLE_NAME AS NAME,
        FINAL_COLUMN AS COLUMN_NAME,
        SOURCE_TABLE AS UPSTREAM_TABLE,
        SOURCE_COLUMNS AS UPSTREAM_COLUMN,
        NULL AS COLUMN_DESCRIPTION,  -- Set column description to NULL for now
        TRANSFORMATION AS REASONING
    FROM COLUMN_LINEAGE_CORTEX
    """

    # Execute the query and load the results into a DataFrame
    df = pd.read_sql(query, conn)

    # Convert relevant columns to uppercase for case-insensitive matching
    df['NAME'] = df['NAME'].str.upper().str.strip()
    df['COLUMN_NAME'] = df['COLUMN_NAME'].str.upper().str.strip()
    df['UPSTREAM_TABLE'] = df['UPSTREAM_TABLE'].str.upper().str.strip()
    df['UPSTREAM_COLUMN'] = df['UPSTREAM_COLUMN'].str.upper().str.strip()

    # Close the connection
    conn.close()

    return df

# Function to extract column name after the dot
def extract_column_name(column_name):
    # Extract the part after the dot, if it exists
    return column_name.split('.')[-1].strip()

# Recursive function to build JSON hierarchy for a given table and column
def build_hierarchy(df, model_name, column_name):
    # Convert input model name and column name to uppercase for consistency
    model_name = model_name.upper().strip()
    column_name = column_name.upper().strip()

    # Initialize the base structure for the current node
    base_structure = {
        "model": model_name,
        "column": column_name,
        "column Description": "",  # Will be set to an empty string for now
        "reasoning": "",  # Field for reasoning
        "upstream_models": []
    }

    # Safely extract the description and reasoning for the current model and column
    try:
        base_structure["column Description"] = df.loc[
            (df['NAME'] == model_name) & (df['COLUMN_NAME'] == column_name), 'COLUMN_DESCRIPTION'
        ].values[0] or "Description not available"
    except IndexError:
        base_structure["column Description"] = "Description not available"

    try:
        base_structure["reasoning"] = df.loc[
            (df['NAME'] == model_name) & (df['COLUMN_NAME'] == column_name), 'REASONING'
        ].values[0] or "Reasoning not available"
    except IndexError:
        base_structure["reasoning"] = "Reasoning not available"

    # Get the row corresponding to the current model and column
    current_row = df[(df['NAME'] == model_name) & (df['COLUMN_NAME'] == column_name)]

    if current_row.empty:
        # If no data is found, return the base structure without upstream models
        return base_structure

    # Extract the upstream tables and columns
    upstream_tables = str(current_row['UPSTREAM_TABLE'].values[0]).split(',')
    upstream_columns = str(current_row['UPSTREAM_COLUMN'].values[0]).split(',')

    # Iterate over each upstream table and column pair
    for upstream_table, upstream_column in zip(upstream_tables, upstream_columns):
        upstream_table = upstream_table.strip().upper()
        # Extract only the column name after the dot
        upstream_column_name = extract_column_name(upstream_column.strip().upper())

        # Build hierarchy for upstream
        upstream_hierarchy = build_hierarchy(df, upstream_table, upstream_column_name)
        if upstream_hierarchy:
            base_structure["upstream_models"].append(upstream_hierarchy)

    return base_structure

# Function to build the entire JSON hierarchy for all columns in the DataFrame
def build_full_hierarchy(df):
    # Initialize an empty list to store the full hierarchy
    full_hierarchy = []

    # Iterate through all unique tables and columns in the DataFrame
    for index, row in df.iterrows():
        model_name = row['NAME']
        column_name = extract_column_name(row['COLUMN_NAME'])

        # Build the hierarchy for each model and column
        hierarchy = build_hierarchy(df, model_name, column_name)

        # Append the hierarchy to the full list
        full_hierarchy.append(hierarchy)

    return full_hierarchy

# Main Function to Execute the Process
def main():
    # Load data from the Snowflake table
    df = read_data_from_snowflake()

    # Build the full JSON hierarchy for all columns
    full_hierarchy = build_full_hierarchy(df)

    # Save the JSON hierarchy to a file
    with open('lineage.json', 'w') as f:
        json.dump(full_hierarchy, f, indent=4)

    print('JSON file created: lineage.json')

# Run the main function
if __name__ == "__main__":
    main()