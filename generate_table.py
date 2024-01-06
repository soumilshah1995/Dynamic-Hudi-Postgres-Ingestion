import psycopg2
from psycopg2 import sql
import random
import string


def create_connection(database, user, password, host, port):
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error: Unable to connect to the database.\n{e}")
        raise


def create_table(connection, table_name):
    try:
        cursor = connection.cursor()

        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id SERIAL PRIMARY KEY,
                column_data TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """).format(sql.Identifier(table_name))

        cursor.execute(create_table_query)
        connection.commit()

        print(f"Table '{table_name}' created successfully.")
    except psycopg2.Error as e:
        print(f"Error: Unable to create table.\n{e}")
        raise


def generate_dummy_data():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))


def insert_dummy_data(connection, table_name):
    try:
        cursor = connection.cursor()

        insert_query = sql.SQL("""
            INSERT INTO {} (column_data) VALUES (%s)
        """).format(sql.Identifier(table_name))

        for _ in range(10):  # Insert 10 rows of dummy data per table
            dummy_data = generate_dummy_data()
            cursor.execute(insert_query, (dummy_data,))

        connection.commit()

        print(f"Dummy data inserted into '{table_name}' successfully.")
    except psycopg2.Error as e:
        print(f"Error: Unable to insert dummy data.\n{e}")
        raise


def main(database, user, password, host, port, num_tables):
    try:
        connection = create_connection(database, user, password, host, port)

        for i in range(1, num_tables + 1):
            table_name = f"table_{i}"
            create_table(connection, table_name)
            insert_dummy_data(connection, table_name)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    # Update these parameters with your PostgreSQL connection details
    database_name = "postgres"
    db_user = "postgres"
    db_password = "postgres"
    db_host = "localhost"
    db_port = "5432"

    # Specify the number of tables to create
    number_of_tables = 30

    main(database_name, db_user, db_password, db_host, db_port, number_of_tables)
