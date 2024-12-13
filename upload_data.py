import os
import csv
import pymysql

class UploadData:
    def __init__(self, db_connection, data_folder="D:\jetbrains\PyCharm 2023.1.2\pythonProject6\data"):
        """
        Initialize the UploadData class.

        Args:
            db_connection: An established database connection.
            data_folder (str): Folder where CSV files are stored.
        """
        self.connection = db_connection
        self.data_folder = data_folder
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)

    def import_data_to_database(self, file_name, table_name):
        """
        Import data from a CSV file in the data folder to a database table.

        Args:
            file_name (str): The name of the file in the data folder.
            table_name (str): The name of the table to create or update.
        """
        file_path = os.path.join(self.data_folder, file_name)

        # Check if the file exists
        if not os.path.exists(file_path):
            print(f"Error: File {file_name} does not exist in the data folder. Please upload the file first.")
            return

        # Process the CSV file
        try:
            with open(file_path, mode='r', encoding='utf-8') as file:
                reader = csv.reader(file)
                headers = next(reader)  # Read header row
                headers = [header.lstrip("\ufeff") for header in headers]
                sample_data = next(reader, None)
                # Create table based on CSV headers
                self._create_table_from_headers(table_name, headers, sample_data)

                # Insert rows into table
                self._insert_data_from_csv(table_name, headers, reader)

            print(f"Data from {file_name} has been successfully imported into table {table_name}.")
        except Exception as e:
            print(f"Failed to import data: {e}")

    def _create_table_from_headers(self, table_name, headers, sample_data):
        """Create a table in the database based on CSV headers."""

        headers = [header.lstrip("\ufeff") for header in headers]

        with self.connection.cursor() as cursor:
            columns = []
            for col_index, header in enumerate(headers):
                # Extract the value from the first row for type inference
                column_value = sample_data[col_index] if sample_data else None

                # Infer type based on the value
                if column_value is None:
                    inferred_type = "VARCHAR(255)"
                elif column_value.isdigit():
                    inferred_type = "INT"
                elif column_value.replace(".", "", 1).isdigit():
                    inferred_type = "FLOAT"
                else:
                    inferred_type = "VARCHAR(255)"

                columns.append(f"`{header}` {inferred_type}")

            columns_sql = ", ".join(columns)
            # Drop table if exists, then create it
            cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")

            create_table_sql = f"CREATE TABLE `{table_name}` ({columns_sql});"
            cursor.execute(create_table_sql)
            self.connection.commit()

    def _insert_data_from_csv(self, table_name, headers, reader):
        """Insert data from CSV reader into the database table."""
        headers = [header.lstrip("\ufeff") for header in headers]

        with self.connection.cursor() as cursor:
            placeholders = ", ".join(["%s"] * len(headers))
            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(headers)}) VALUES ({placeholders})"
            for row in reader:
                cursor.execute(insert_sql, row)
            self.connection.commit()

    def list_uploaded_files(self):
        """List all files in the data folder."""
        files = os.listdir(self.data_folder)
        if files:
            print("Available files in the data folder:")
            for file in files:
                print(f"- {file}")
        else:
            print("No files found in the data folder.")

        return files
