import json
import pandas as pd
import snowflake.connector
import os
from collections import defaultdict
from dotenv import load_dotenv
import re

# Load environment variables from .env file
load_dotenv()

# Step 1: Load JSON Files
def load_manifest(file_path):
    """Load manifest.json and return the 'nodes' dictionary."""
    try:
        with open(file_path, 'r') as file:
            manifest = json.load(file)
        return manifest.get('nodes', {})
    except Exception as e:
        print(f"Error loading manifest file: {e}")
        return {}

def load_catalog(file_path):
    """Load catalog.json and return the 'nodes' and 'sources' dictionaries."""
    try:
        with open(file_path, 'r') as file:
            catalog = json.load(file)
        return catalog.get('nodes', {}), catalog.get('sources', {})
    except Exception as e:
        print(f"Error loading catalog file: {e}")
        return {}, {}

# Step 2: Process Nodes and Build DataFrame
def build_dataframe_from_manifest(manifest_nodes, catalog_nodes, catalog_sources):
    """
    Process each node from manifest and extract required information
    to build a DataFrame for Snowflake insertion.
    """
    data = []

    for node_key, node_info in manifest_nodes.items():
        # Extract required fields from manifest
        database = node_info.get('database', '')
        schema = node_info.get('schema', '')
        table_name = node_info.get('name', '')
        resource_type = node_info.get('resource_type', '')
        raw_code = node_info.get('raw_code', '')
        description = node_info.get('description', '')  # If available

        # Debug: Print current node being processed
        print(f"Processing node: {node_key} (Resource Type: {resource_type})")

        # Depending on resource_type, search in catalog_nodes or catalog_sources
        if resource_type == 'model':
            catalog_entry = catalog_nodes.get(node_key, {})
        elif resource_type == 'source':
            catalog_entry = catalog_sources.get(node_key, {})
        else:
            print(f"Unsupported resource_type '{resource_type}' for node '{node_key}'. Skipping.")
            continue  # Skip unsupported resource types

        if not catalog_entry:
            print(f"Warning: No catalog entry found for node '{node_key}'. Skipping.")
            continue  # Skip nodes without catalog entries

        # Retrieve columns from catalog_entry
        columns = catalog_entry.get('columns', {})
        if not columns:
            print(f"Warning: No columns found for node '{node_key}' in catalog. Skipping.")
            continue  # Skip nodes without columns

        # Process dependencies to build reference information
        depends_on = node_info.get('depends_on', {})
        dependent_nodes = depends_on.get('nodes', [])

        reference_info = defaultdict(list)  # {dependency_full_name: [columns]}

        for dep in dependent_nodes:
            # Dependency node key format can vary:
            # For models: "model.package.table_name" (3 parts)
            # For sources: "source.package.source_name.table_name" (4 parts)
            dep_parts = dep.split('.')
            dep_resource_type = dep_parts[0]

            if dep_resource_type == 'model':
                if len(dep_parts) != 3:
                    print(f"Warning: Unexpected model dependency format '{dep}'. Skipping.")
                    continue  # Skip unexpected formats
                _, dep_package, dep_name = dep_parts
                dep_full_key = dep  # e.g., "model.jaffle_shop.stg_products"

                # Look up in catalog_nodes
                dep_catalog_entry = catalog_nodes.get(dep_full_key, {})
                if not dep_catalog_entry:
                    print(f"Warning: No catalog entry found for dependency '{dep_full_key}'. Skipping.")
                    continue  # Skip dependencies without catalog entries

                dep_columns = dep_catalog_entry.get('columns', {})
                if not dep_columns:
                    print(f"Warning: No columns found for dependency '{dep_full_key}'. Skipping.")
                    continue  # Skip dependencies without columns

                # Extract database and schema from catalog_entry
                dep_database = dep_catalog_entry.get('metadata', {}).get('database', database)  # Fallback to current node's database
                dep_schema = dep_catalog_entry.get('metadata', {}).get('schema', schema)  # Fallback to current node's schema
                dep_table_name = dep_catalog_entry.get('name', dep_name)

                dep_full_name = f"{dep_database}.{dep_schema}.{dep_table_name}"

                # Add columns to reference_info
                for dep_col in dep_columns:
                    reference_info[dep_full_name].append(dep_col)

            elif dep_resource_type == 'source':
                if len(dep_parts) != 4:
                    print(f"Warning: Unexpected source dependency format '{dep}'. Skipping.")
                    continue  # Skip unexpected formats
                _, dep_package, dep_source, dep_name = dep_parts
                dep_full_key = dep  # e.g., "source.jaffle_shop.ecom.raw_products"

                # Look up in catalog_sources
                dep_catalog_entry = catalog_sources.get(dep_full_key, {})
                if not dep_catalog_entry:
                    print(f"Warning: No catalog entry found for dependency '{dep_full_key}'. Skipping.")
                    continue  # Skip dependencies without catalog entries

                dep_columns = dep_catalog_entry.get('columns', {})
                if not dep_columns:
                    print(f"Warning: No columns found for dependency '{dep_full_key}'. Skipping.")
                    continue  # Skip dependencies without columns

                # Extract database and schema from catalog_entry
                dep_database = dep_catalog_entry.get('metadata', {}).get('database', database)  # Fallback to current node's database
                dep_schema = dep_catalog_entry.get('metadata', {}).get('schema', schema)  # Fallback to current node's schema
                dep_table_name = dep_catalog_entry.get('name', dep_name)

                dep_full_name = f"{dep_database}.{dep_schema}.{dep_table_name}"

                # Add columns to reference_info
                for dep_col in dep_columns:
                    reference_info[dep_full_name].append(dep_col)
            else:
                print(f"Warning: Unsupported dependency resource_type '{dep_resource_type}' in '{dep}'. Skipping.")
                continue  # Skip unsupported resource types

        # Convert reference_info to JSON string
        reference_str = json.dumps(reference_info)

        # Iterate through columns and build data entries
        for column_name, column_info in columns.items():
            column_description = column_info.get('description', '')  # Get column description

            # Construct unique_key
            unique_key = f"{database}.{schema}.{table_name}.{column_name}"

            # Append data entry
            data.append({
                'unique_key': unique_key,
                'database': database,
                'schema': schema,
                'table_name': table_name,
                'column_name': column_name,
                'column_description': column_description,
                'resource_type': resource_type,
                'name': table_name,  # Assuming 'name' refers to table name
                'sql': raw_code,
                'reference': reference_str
            })

    # Create DataFrame
    df = pd.DataFrame(data)

    # Debug: Print DataFrame info
    print(f"DataFrame constructed with {len(df)} rows and {len(df.columns)} columns.")
    print("DataFrame Columns:", df.columns.tolist())
    print("Sample Data:")
    print(df.head())

    return df

# Step 3: Update SQL Statements
def replace_refs_and_sources_in_sql(sql, reference_str):
    """
    Replace ref() and source() in SQL with actual table names using reference information.
    """
    try:
        reference_data = json.loads(reference_str)
    except json.JSONDecodeError as e:
        print(f"Error decoding reference JSON: {e}")
        return sql  # Return original SQL if JSON is invalid

    # Function to replace ref()
    def ref_replacer(match):
        ref_table = match.group(1)
        for full_name, cols in reference_data.items():
            table_name = full_name.split('.')[-1]
            if ref_table == table_name:
                return full_name  # Replace with full table name
        return match.group(0)  # No replacement found

    # Function to replace source()
    def source_replacer(match):
        source_name = match.group(1)
        table_name = match.group(2)
        for full_name, cols in reference_data.items():
            # Assuming source full name format: "database.schema.table"
            parts = full_name.split('.')
            if len(parts) != 3:
                continue
            src_database, src_schema, src_table = parts
            if src_table == table_name:
                return full_name  # Replace with full table name
        return match.group(0)  # No replacement found

    # Replace ref() patterns
    sql = re.sub(r"{{\s*ref\('([^']+)'\)\s*}}", ref_replacer, sql)

    # Replace source() patterns
    sql = re.sub(r"{{\s*source\('([^']+)'\s*,\s*'([^']+)'\)\s*}}", source_replacer, sql)

    return sql

def update_sql_column(df):
    """
    Update the 'sql' column in the DataFrame by replacing ref() and source() with actual table names.
    """
    def replace_sql(row):
        sql = row['sql']
        reference_str = row['reference']
        if pd.isna(sql) or not sql:
            return sql  # Return as is if SQL is empty or NaN
        if pd.isna(reference_str) or not reference_str:
            return sql  # Return as is if reference is empty or NaN
        return replace_refs_and_sources_in_sql(sql, reference_str)

    # Apply the replacement
    df['sql'] = df.apply(replace_sql, axis=1)
    return df

# Step 4: Connect to Snowflake and Insert Data
def connect_to_snowflake():
    """Establish a connection to Snowflake using environment variables."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account=os.getenv('account'),
            warehouse=os.getenv('warehouse'),
            database=os.getenv('database'),
            schema=os.getenv('schema'),
            role=os.getenv('role')
        )
        print("Snowflake connection established successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def insert_data_to_snowflake(conn, df):
    """
    Insert data from the DataFrame into the Snowflake table.
    Assumes the target table exists with the appropriate schema.
    """
    if conn is None:
        print("Snowflake connection is not established. Aborting insertion.")
        return

    # Define the target table name
    target_table = "TABLE_SCHEMA_REF"  # Replace with your actual table name

    # Define the truncate query
    truncate_table_query = f"TRUNCATE TABLE {target_table};"

    # Define the insert query
    insert_query = f"""
    INSERT INTO {target_table}
    (unique_key, database, schema, table_name, column_name, column_description, resource_type, name, sql, reference)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Extract data as list of tuples
    try:
        data_to_insert = df[['unique_key', 'database', 'schema', 'table_name', 'column_name',
                             'column_description', 'resource_type', 'name', 'sql', 'reference']].values.tolist()
    except KeyError as e:
        print(f"DataFrame is missing required columns: {e}")
        return

    cursor = conn.cursor()
    try:
        # Use environment variables in SQL commands
        cursor.execute(f"USE WAREHOUSE {os.getenv('warehouse')};")  # Explicitly set the warehouse
        cursor.execute(f"USE DATABASE {os.getenv('database')};")
        cursor.execute(f"USE SCHEMA {os.getenv('schema')};")

        # Truncate the table before inserting
        cursor.execute(truncate_table_query)
        print(f"Table '{target_table}' truncated successfully.")

        # Insert data into Snowflake table
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"Inserted {len(data_to_insert)} rows into '{target_table}' successfully.")
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")
        conn.rollback()
    finally:
        cursor.close()

# Main Function to Execute the Process
def main():
    # Define paths to manifest.json and catalog.json
    manifest_path = 'manifest.json'  # Replace with your actual path
    catalog_path = 'catalog.json'    # Replace with your actual path

    # Load JSON files
    manifest_nodes = load_manifest(manifest_path)
    catalog_nodes, catalog_sources = load_catalog(catalog_path)

    if not manifest_nodes:
        print("No nodes found in manifest. Aborting process.")
        return

    if not catalog_nodes and not catalog_sources:
        print("No nodes or sources found in catalog. Aborting process.")
        return

    # Build the DataFrame from manifest and catalog
    df = build_dataframe_from_manifest(manifest_nodes, catalog_nodes, catalog_sources)

    if df.empty:
        print("The DataFrame is empty. No data to insert. Aborting process.")
        return

    # Update the SQL column by replacing ref() and source()
    df = update_sql_column(df)

    # Connect to Snowflake
    conn = connect_to_snowflake()
    if conn is None:
        print("Failed to connect to Snowflake. Aborting process.")
        return

    # Insert data into Snowflake
    insert_data_to_snowflake(conn, df)

    # Close Snowflake connection
    conn.close()
    print("Snowflake connection closed.")

    print("Data has been successfully processed and inserted into Snowflake.")

if __name__ == "__main__":
    main()
