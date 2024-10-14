import snowflake.connector
import json
import sqlglot
from sqlglot import parse_one, exp
from dotenv import load_dotenv
import os

load_dotenv()

# Connect to Snowflake
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv('user'),
        password=os.getenv('password'),
        account=os.getenv('account'),
        warehouse=os.getenv('warehouse'),
        database=os.getenv('database'),
        schema=os.getenv('schema'),
        role=os.getenv('role')
    )
    return conn

# Function to read the SQL and reference columns from Snowflake
def fetch_column_lineage_data(conn):
    cursor = conn.cursor()
    query = "SELECT UNIQUE_KEY, SQL, REFERENCE FROM TABLE_SCHEMA_REF"
    cursor.execute(query)
    rows = cursor.fetchall()
    data = [{'unique_key': row[0], 'sql': row[1], 'reference': row[2]} for row in rows]
    cursor.close()
    return data

# Function to update the expanded SQL in Snowflake
def update_expanded_sql(conn, unique_key, expanded_sql):
    cursor = conn.cursor()
    query = """
    UPDATE TABLE_SCHEMA_REF
    SET EXPANDED_SQL = %s
    WHERE UNIQUE_KEY = %s
    """
    cursor.execute(query, (expanded_sql, unique_key))
    conn.commit()
    cursor.close()

# Function to generate the expanded SQL
def generate_expanded_sql(original_sql, reference_str):
    # Convert the reference string back to dictionary format
    schema = json.loads(reference_str)

    # Parse the SQL
    parsed = parse_one(original_sql)

    # Dictionary to hold CTEs and their columns
    cte_columns = {}

    # Function to extract fully qualified table name
    def get_fully_qualified_name(table_expression):
        parts = []
        if table_expression.args.get("catalog"):
            parts.append(table_expression.args["catalog"].name)
        if table_expression.args.get("db"):
            parts.append(table_expression.args["db"].name)
        if table_expression.args.get("this"):
            parts.append(table_expression.args["this"].name)
        return ".".join(parts)

    # Function to replace * with actual columns in a Select expression
    def replace_star_in_select(select_expr, current_cte_columns, cte_columns):
        new_expressions = []
        for projection in select_expr.expressions:
            if isinstance(projection, exp.Star):
                # Only expand * when explicitly used
                for col in current_cte_columns:
                    new_expressions.append(exp.to_identifier(col))
            elif isinstance(projection, exp.Column) and projection.args.get('table'):
                # Handle table_name.* case (e.g., orders.*)
                table_name = projection.args['table'].name
                if projection.name == "*":
                    # If table_name.* is used, expand it with the CTE's columns
                    if table_name in cte_columns:
                        for col in cte_columns[table_name]:
                            new_expressions.append(exp.column(col, table=table_name))
                    else:
                        new_expressions.append(projection)
                else:
                    # Keep the projection as is for specific column references
                    new_expressions.append(projection)
            else:
                new_expressions.append(projection)
        # Update the select expression with new projections
        select_expr.set("expressions", new_expressions)

    # Function to find the outermost SELECT statement
    def find_outer_select(expression):
        if isinstance(expression, exp.Select):
            return expression
        for arg in expression.args.values():
            if isinstance(arg, list):
                for item in arg:
                    result = find_outer_select(item)
                    if result:
                        return result
            elif isinstance(arg, exp.Expression):
                result = find_outer_select(arg)
                if result:
                    return result
        return None

    # Process each CTE
    with_expression = parsed.args.get("with")
    if not with_expression:
        raise ValueError("No WITH clause found in the SQL.")

    # Process each CTE in the WITH clause
    for cte in with_expression.expressions:
        cte_name = cte.alias
        select_expr = cte.this
        from_expr = select_expr.args.get("from")

        if not from_expr:
            raise ValueError(f"No FROM clause found in CTE '{cte_name}'.")

        # Handle single table or CTE in FROM clause
        from_table = from_expr.find(exp.Table)
        if not from_table:
            raise NotImplementedError(f"CTE '{cte_name}' has a complex FROM clause which is not supported.")

        # Extract the fully qualified table name or CTE reference
        source_full = get_fully_qualified_name(from_table)

        # Determine columns from the source (schema or another CTE)
        if source_full in schema:
            source_columns = schema[source_full]
        elif source_full in cte_columns:
            source_columns = cte_columns[source_full]
        else:
            raise ValueError(f"Source '{source_full}' not found in schema or previously defined CTEs.")

        # Replace * in the select expression
        replace_star_in_select(select_expr, source_columns, cte_columns)

        # Extract the column names after replacement
        new_columns = []
        for projection in select_expr.expressions:
            if isinstance(projection, exp.Alias):
                new_columns.append(projection.alias_or_name)
            elif isinstance(projection, exp.Column):
                new_columns.append(projection.name)
            else:
                if projection.alias:
                    new_columns.append(projection.alias)
                else:
                    new_columns.append(projection.sql())

        # Store the columns for this CTE in cte_columns
        cte_columns[cte_name] = new_columns

    # Replace * in the final SELECT
    final_select = find_outer_select(parsed)
    if not final_select:
        raise ValueError("No final SELECT statement found in the SQL.")

    # Find the last CTE as the source for the final SELECT
    last_cte_name = list(cte_columns.keys())[-1]
    final_columns = cte_columns.get(last_cte_name, [])

    # Replace * in the final SELECT
    replace_star_in_select(final_select, final_columns, cte_columns)

    # Generate the expanded SQL using the sql() method instead of to_sql()
    expanded_sql = parsed.sql(pretty=True).upper()
    return expanded_sql

# Main function to execute the process
def main():
    # Connect to Snowflake
    conn = connect_to_snowflake()

    # Fetch SQL and reference data from COLUMN_LINEAGE
    lineage_data = fetch_column_lineage_data(conn)

    # Loop through each record, generate expanded SQL, and update the table
    for record in lineage_data:
        unique_key = record['unique_key']
        original_sql = record['sql']
        reference_str = record['reference']

        # Generate expanded SQL
        try:
            expanded_sql = generate_expanded_sql(original_sql, reference_str)
            # Update the EXPANDED_SQL column in Snowflake
            update_expanded_sql(conn, unique_key, expanded_sql)
            print(f"Updated expanded SQL for unique_key: {unique_key}")
        except Exception as e:
            print(f"Error processing unique_key {unique_key}: {str(e)}")

    # Close the Snowflake connection
    conn.close()

if __name__ == "__main__":
    main()
