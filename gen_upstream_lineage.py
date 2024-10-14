from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import call_builtin
import json
import re
import logging

# Configure logging for better debugging and visibility
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Get the active Snowpark session (in a Snowflake notebook, the session is usually already available)
session = Session.builder.getOrCreate()

# Define the table names (replace with your actual table names if different)
TABLE_SCHEMA_REF = 'JAFFLE_LINEAGE.LINEAGE_DATA.TABLE_SCHEMA_REF'
COLUMN_LINEAGE_CORTEX = 'JAFFLE_LINEAGE.LINEAGE_DATA.COLUMN_LINEAGE_CORTEX'

# Fetch distinct combinations of DATABASE, SCHEMA, TABLE_NAME, COLUMN_NAME, EXPANDED_SQL
df = session.sql(f"""
    SELECT DISTINCT DATABASE, SCHEMA, TABLE_NAME, COLUMN_NAME, EXPANDED_SQL
    FROM {TABLE_SCHEMA_REF}
    WHERE EXPANDED_SQL IS NOT NULL
""")

rows = df.collect()

for row in rows:
    input_record = row.as_dict()
    sql_query = input_record['EXPANDED_SQL']
    
    composite_key = {
        'DATABASE_NAME': input_record['DATABASE'],
        'SCHEMA_NAME': input_record['SCHEMA'],
        'TABLE_NAME': input_record['TABLE_NAME'],
        'FINAL_COLUMN': input_record['COLUMN_NAME']
    }
    
    # Check if the record already exists in the target table
    check_query = f"""
        SELECT EXPANDED_SQL
        FROM {COLUMN_LINEAGE_CORTEX}
        WHERE DATABASE_NAME = '{composite_key['DATABASE_NAME']}'
        AND SCHEMA_NAME = '{composite_key['SCHEMA_NAME']}'
        AND TABLE_NAME = '{composite_key['TABLE_NAME']}'
        AND FINAL_COLUMN = '{composite_key['FINAL_COLUMN']}'
    """
    
    existing_record_df = session.sql(check_query)
    existing_records = existing_record_df.collect()
    
    if existing_records:
        existing_sql = existing_records[0]['EXPANDED_SQL']
        if existing_sql == sql_query:
            logging.info(f"SQL unchanged for {composite_key}. Skipping record.")
            continue  # Skip processing if the SQL is the same
        else:
            logging.info(f"SQL changed for {composite_key}. Deleting old records.")
            # SQL has changed, so delete existing records with this composite key
            delete_query = f"""
                DELETE FROM {COLUMN_LINEAGE_CORTEX}
                WHERE DATABASE_NAME = '{composite_key['DATABASE_NAME']}'
                AND SCHEMA_NAME = '{composite_key['SCHEMA_NAME']}'
                AND TABLE_NAME = '{composite_key['TABLE_NAME']}'
                AND FINAL_COLUMN = '{composite_key['FINAL_COLUMN']}'
            """
            try:
                session.sql(delete_query).collect()
            except Exception as e:
                logging.error(f"Error deleting existing records: {e}")
                continue  # Skip to next record
    else:
        logging.info(f"New record for {composite_key}. Processing.")
    
    # Now process the SQL query using Cortex LLM to generate lineage information
    # Construct the prompt for the Cortex LLM
    prompt = (
        "You are an expert in SQL lineage analysis. "
        "Given the following SQL query, identify the source tables and columns for each final column in the SELECT statement. "
        "Additionally, provide simple reasoning in business-friendly language explaining the transformation for each column. "
        "Provide the results as a JSON array of objects, where each object has the keys: FINAL_COLUMN, SOURCE_TABLE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_COLUMNS, and REASONING. "
        "SQL Query: " + sql_query
    )

    # Escape single quotes in the prompt
    prompt = prompt.replace("'", "''")

    # Call the Cortex LLM using the SNOWFLAKE.CORTEX.COMPLETE function
    try:
        lineage_response_df = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-405b',  -- You can change the model if needed
                '{prompt}'
            ) AS LINEAGE_RESPONSE
        """)
        lineage_response_row = lineage_response_df.collect()[0]
        lineage_response = lineage_response_row['LINEAGE_RESPONSE']
    except Exception as e:
        logging.error(f"Error calling Cortex LLM: {e}")
        continue  # Skip to the next SQL query

    # Try to extract the JSON data from the response
    try:
        # Use regex to extract JSON array from the response
        json_match = re.search(r'\[.*\]', lineage_response, re.DOTALL)
        if json_match:
            json_data = json_match.group(0)
            parsed_records = json.loads(json_data)
        else:
            # Try to parse the entire response if it is valid JSON
            parsed_records = json.loads(lineage_response)
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON response: {e}")
        logging.error(f"Response: {lineage_response}")
        continue  # Skip to the next SQL query

    # Insert the parsed records into the COLUMN_LINEAGE_CORTEX table
    for record in parsed_records:
        # Prepare the data for insertion
        insert_data = {
            'DATABASE_NAME': composite_key['DATABASE_NAME'],
            'SCHEMA_NAME': composite_key['SCHEMA_NAME'],
            'TABLE_NAME': composite_key['TABLE_NAME'],
            'REFERENCE': None,  # If REFERENCE is needed, you can fetch it from source if available
            'EXPANDED_SQL': sql_query,
            'FINAL_COLUMN': record.get('FINAL_COLUMN', 'Unknown'),
            'SOURCE_TABLE': record.get('SOURCE_TABLE', 'Unknown'),
            'SOURCE_DATABASE': record.get('SOURCE_DATABASE', 'Unknown'),
            'SOURCE_SCHEMA': record.get('SOURCE_SCHEMA', 'Unknown'),
            'SOURCE_COLUMNS': ', '.join(record.get('SOURCE_COLUMNS', [])) if isinstance(record.get('SOURCE_COLUMNS'), list) else record.get('SOURCE_COLUMNS', 'Unknown'),
            'REASONING': record.get('REASONING', 'Unknown')
        }

        # Convert the data to a DataFrame
        insert_df = session.create_dataframe([insert_data])

        # Write the DataFrame to the COLUMN_LINEAGE_CORTEX table
        try:
            insert_df.write.mode('append').save_as_table(COLUMN_LINEAGE_CORTEX)
            logging.info(f"Inserted/Updated record for FINAL_COLUMN: {insert_data['FINAL_COLUMN']}")
        except Exception as e:
            logging.error(f"Error inserting record into {COLUMN_LINEAGE_CORTEX}: {e}")
            logging.error(f"Record data: {json.dumps(insert_data)}")
            continue  # Skip to the next record

logging.info("Processing completed.")
