import mysql.connector
import clickhouse_connect
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def map_mysql_to_clickhouse_type(mysql_type):
    """Maps MySQL data types to ClickHouse data types."""
    if 'int' in mysql_type: return 'Int64'
    elif 'varchar' in mysql_type or 'text' in mysql_type: return 'String'
    elif 'date' in mysql_type: return 'Date32'
    elif 'datetime' in mysql_type or 'timestamp' in mysql_type: return 'DateTime'
    elif 'decimal' in mysql_type: return 'Decimal(18, 2)' # Adjust precision and scale as needed
    elif 'float' in mysql_type or 'double' in mysql_type: return 'Float64'
    elif 'tinyint(1)' in mysql_type: return 'Boolean' # Map tinyint(1) to boolean
    else:
        logging.warning(f"Unknown MySQL type: {mysql_type}. Defaulting to String.")
        return 'String'  # Default to String for unknown types
    
def mysql_to_clickhouse(mysql_config, clickhouse_config, tables):
    # mysql
    mysql_conn = mysql.connector.connect(**mysql_config)
    mysql_cursor = mysql_conn.cursor(dictionary=True)

    # clickhouse
    clickhouse_client = clickhouse_connect.create_client(**clickhouse_config)

    for table in tables:
        # get all columns name from mysql
        mysql_cursor.execute(f"DESCRIBE {table}")
        mysql_columns = mysql_cursor.fetchall()

        clickhouse_columns = []
        for column in mysql_columns:
            mysql_type = column['Type'].lower()
            clickhouse_type = map_mysql_to_clickhouse_type(mysql_type)
            #by default clickhouse is not nullable, so we need to be explicit
            clickhouse_columns.append(f"`{column['Field']}` Nullable({clickhouse_type})")

        clickhouse_client.command(f"DROP TABLE IF EXISTS {table}")
        clickhouse_create_table_query = f"""
            CREATE OR REPLACE TABLE {table} (
                {', '.join(clickhouse_columns)}
            ) ENGINE = MergeTree()
            PRIMARY KEY tuple()
        """
        clickhouse_client.command(clickhouse_create_table_query)
        logging.info(f"Created table: {table}")


        # fetch every 100,000 rows
        mysql_cursor.execute(f"SELECT * FROM {table}")
        while True:
            rows = mysql_cursor.fetchmany(size=100000)  # Fetch data in batches
            if not rows:
                break

            clickhouse_data = []
            for row in rows:
                clickhouse_data.append(tuple(row.values()))  # use tuple format for insertion

            clickhouse_client.insert(table=table,data=clickhouse_data,column_names=[column['Field'] for column in mysql_columns])
            logging.info(f"Inserted {len(clickhouse_data)} rows into {table}")

    mysql_cursor.close()
    mysql_conn.close()
    clickhouse_client.close()
    logging.info("Finished processing tables.")



