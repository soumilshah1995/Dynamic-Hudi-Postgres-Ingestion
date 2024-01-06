import subprocess
import psycopg2
from psycopg2 import sql
import prettytable
import logging
import yaml
import concurrent.futures

logging.basicConfig(level=logging.INFO)


def get_primary_key(cursor, schema, table_name):
    query = """
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    cursor.execute(query, (schema, table_name))

    primary_keys = cursor.fetchall()
    return ', '.join([key[0] for key in primary_keys]) if primary_keys else 'N/A'


def create_trigger(cursor, schema, table_name):
    primary_key = get_primary_key(cursor, schema, table_name)

    if primary_key == 'N/A':
        logging.warning(f"No primary key found for table: {table_name}")
        return

    trigger_function = f"""
        CREATE OR REPLACE FUNCTION update_{table_name}_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """

    trigger_creation = f"""
        CREATE TRIGGER update_{table_name}_updated_at_trigger
        BEFORE UPDATE ON {schema}.{table_name}
        FOR EACH ROW
        EXECUTE FUNCTION update_{table_name}_updated_at();
    """

    cursor.execute(trigger_function)
    cursor.execute(trigger_creation)

    logging.info(
        f"Executing trigger function creation for table: {table_name} in schema: {schema} with primary key: {primary_key}")
    logging.info(
        f"Executing trigger creation for table: {table_name} in schema: {schema} with primary key: {primary_key}")


def trigger_exists(cursor, schema, table_name):
    cursor.execute(sql.SQL("SELECT 1 FROM information_schema.triggers WHERE trigger_name = %s"),
                   (f'update_{table_name}_updated_at_trigger',))
    return cursor.fetchone() is not None


def execute_spark_submit_parallel(spark_submit_command):
    try:
        logging.info(f"Executing Spark SUBMIT command:\n{spark_submit_command}")
        subprocess.run(spark_submit_command, shell=True, check=True)
        logging.info("Spark SUBMIT command executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing Spark SUBMIT command: {e}")


def generate_spark_submit_job(config, table_name, primary_key):
    target_base_path = config['target-base-path'] + table_name
    print("\n")
    spark_submit_template = """spark-submit \\
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \\
  --packages '{packages}' \\
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \\
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \\
  --conf "spark.sql.hive.convertMetastoreParquet=false" \\
  --master 'local[*]'  \\
  --executor-memory 1g  \\
   {jar_path}  \\
  --table-type {table_type}  \\
  --op {operation}  \\
  --source-ordering-field updated_at  \\
  --source-class org.apache.hudi.utilities.sources.JdbcSource  \\
  --target-base-path {target_base_path}  \\
  --target-table {table_name}  \\
  --transformer-class org.apache.hudi.utilities.transform.SqlQueryBasedTransformer  \\
  --hoodie-conf "hoodie.datasource.write.recordkey.field={primary_key}"  \\
  --hoodie-conf "hoodie.datasource.write.partitionpath.field=year,month,day"  \\
  --hoodie-conf "hoodie.datasource.write.precombine.field=updated_at"  \\
  --hoodie-conf "hoodie.streamer.jdbc.url=jdbc:postgresql://{host}:{port}/{database}"  \\
  --hoodie-conf "hoodie.streamer.jdbc.user={user}"  \\
  --hoodie-conf "hoodie.streamer.jdbc.password={password}"  \\
  --hoodie-conf "hoodie.streamer.jdbc.driver.class=org.postgresql.Driver"  \\
  --hoodie-conf "hoodie.streamer.jdbc.table.name={table_name}"  \\
  --hoodie-conf "hoodie.streamer.jdbc.table.incr.column.name=updated_at"  \\
  --hoodie-conf "hoodie.streamer.jdbc.incr.pull=true"  \\
  --hoodie-conf "hoodie.streamer.jdbc.incr.fallback.to.full.fetch=true"  \\
  --hoodie-conf "hoodie.deltastreamer.transformer.sql=SELECT * ,extract(year from updated_at) as year, extract(month from updated_at) as month, extract(day from updated_at) as day FROM <SRC> a;"
"""

    spark_submit_command = spark_submit_template.format(
        packages=config['spark_submit_options']['packages'],
        jar_path=config['jar'],
        table_type=config['spark_submit_options']['table_type'],
        operation=config['spark_submit_options']['operation'],
        target_base_path=target_base_path,
        table_name=table_name,
        primary_key=primary_key,
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password']
    )

    print(spark_submit_command)
    print("\n")
    return spark_submit_command


def main(config):
    connection = None  # Initialize connection to None

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            user=config['user'],
            password=config['password'],
            host=config['host'],
            database=config['database']
        )

        cursor = connection.cursor()

        # Fetch all tables in the specified schema
        cursor.execute(sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = %s"),
                       (config['schema'],))
        tables = [record[0] for record in cursor.fetchall()]

        # Create a PrettyTable for table formatting
        table = prettytable.PrettyTable()
        table.field_names = ["Sr No", "Table Name", "Schema", "Primary Key", "Trigger"]

        # List to store Spark submit commands
        spark_submit_commands = []

        # Loop through tables
        for idx, table_name in enumerate(tables, start=1):
            # Check if the table should be skipped
            if config['skip_table'] and table_name in config['skip_table'].split(','):
                table.add_row([idx, table_name, config['schema'], 'N/A', 'N/A'])
            else:
                # Check if trigger exists for the table
                if trigger_exists(cursor, config['schema'], table_name):
                    primary_key = get_primary_key(cursor, config['schema'], table_name)
                    table.add_row([idx, table_name, config['schema'], primary_key, 'Trigger Exists'])
                    # Generate Spark SUBMIT job
                    spark_submit_command = generate_spark_submit_job(config, table_name, primary_key)
                    spark_submit_commands.append(spark_submit_command)  # Add to the list

                else:
                    # Create trigger for the table
                    create_trigger(cursor, config['schema'], table_name)
                    # Get primary key of the table
                    primary_key = get_primary_key(cursor, config['schema'], table_name)
                    table.add_row([idx, table_name, config['schema'], primary_key, 'Created'])

                    spark_submit_command = generate_spark_submit_job(config, table_name, primary_key)
                    spark_submit_commands.append(spark_submit_command)  # Add to the list

        # Use concurrent.futures to execute Spark submit commands in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=config['threads']) as executor:
            executor.map(execute_spark_submit_parallel, spark_submit_commands)

        # Commit changes and close the connection
        connection.commit()

        print("\n")
        print(table)
    except Exception as e:
        logging.error(f"Error: {e}")

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    with open('config.yml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    main(config)
