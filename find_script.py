from clickhouse_driver import Client
import subprocess
from datetime import datetime

def search_large_files_and_insert():
    # Size in bit
    min_file_size = 1024

    start_dir = "/"
    find_cmd = f"find {start_dir} -type f -size +{min_file_size}c"
    find_res = subprocess.run(find_cmd, shell=True, stdout=subprocess.PIPE, text=True)

    # Generate current date
    date_today = datetime.today().strftime("%Y-%m-%d")

    # Prepare data for ClickHouse insertion
    data_to_insert = []
    for file_path in find_res.stdout.splitlines():
        # Check if file path contains quotes
        if "'" in file_path:
            ls_cmd = f"ls -lah {file_path}"
        else:
            ls_cmd = f"ls -lah '{file_path}'"
        try:
            ls_res = subprocess.run(ls_cmd, shell=True, stdout=subprocess.PIPE, text=True, check=True)
            ls_data = ls_res.stdout.splitlines()[0].split(maxsplit=8)  # Split only first 8 parts for saving construction of file path with spaces
            user = ls_data[2]
            size = int(''.join(filter(str.isdigit, ls_data[4])))
            file = ' '.join(ls_data[8:])  # Join remaining parts back to form full file name
            data_to_insert.append((user, size, file, date_today))
        except subprocess.CalledProcessError as e:
            print(f"Skipping {file_path}: {e}")
            #pass

    # ClickHouse parameters
    clickhouse_host = "localhost"
    clickhouse_port = 9000
    clickhouse_user = "default"
    clickhouse_password = "default"
    clickhouse_database = "default"
    clickhouse_table = "clickhouse_table"

    # Connect to ClickHouse
    client = Client(host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password, database=clickhouse_database)

    try:
        # Prepare ClickHouse insert query
        insert_query = f"INSERT INTO {clickhouse_table} (user, sizeKb, file, date) VALUES"
        for data in data_to_insert:
            insert_query += f" ('{data[0]}', {data[1]}, '{data[2]}', '{data[3]}'),"
        insert_query = insert_query.rstrip(',')

        # Insert data into ClickHouse
        client.execute(insert_query)

        print("Data inserted successfully into ClickHouse.")

    except Exception as e:
        print(f"Failed to insert data into ClickHouse. Error: {e}")

    finally:
        # Close the connection
        client.disconnect()

search_large_files_and_insert()
