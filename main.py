# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pymysql
from upload_data import UploadData
from query_generator import QueryGenerator
from general import password, database
import os

class ChatDB:
    def __init__(self, db_connection):
        self.connection = db_connection

    def execute_query(self, query):
        """Execute a SQL query and return the results."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"Query execution failed: {e}")
            return None

    def explain_query(self, table, query):
        explanation = ''
        if "SELECT" in query.upper():
            explanation = f"This query retrieves data from {table}"
            if "WHERE" in query.upper():
                explanation += " It includes conditions to filter the results."
            if "GROUP BY" in query.upper():
                explanation += " The results are grouped by specific columns."
            if "LIMIT" in query.upper():
                explanation += " The number of returned rows is limited."

        return explanation

    def close_connection(self):
        """Close the database connection."""
        self.connection.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Initialize ChatDB and UploadData
    db_connection = pymysql.connect(
        host="localhost",
        user="root",
        password=password,
        database=database
    )
    chatdb = ChatDB(db_connection)
    upload_data = UploadData(db_connection=db_connection)

    # List available files in the data folder

    csv_files = upload_data.list_uploaded_files()
    print("CSV files to upload:", csv_files)

    for file_name in csv_files:
        table_name = os.path.splitext(file_name)[0]
        upload_data.import_data_to_database(file_name, table_name)

    print('successfully import the data to databases')

    generator = QueryGenerator(db_connection)
    operations = generator.keywords
    schema, df = generator.get_tables_and_columns()
    tables = schema.keys()

    while True:
        print("Available tables:")
        for table_name in schema.keys():
            print(f"- {table_name}")

        selected_table = input("Enter the name of the table you want to explore: ").strip()
        if selected_table not in schema:
            print("Please Enter a Valid table name")
            continue

        operation = input("Enter the operation type you want: 1:explore table, 2:random, 3:keyword examples, 4:NLP, please enter 1,2,3,4: ").strip()
        if not operation.isdigit() or (int(operation) < 1 and int(operation) > 4):
            print("Please Enter a valid values: from 1, 2, 3, 4")
            continue

        operation = int(operation)

        if operation == 1:
            print(df[selected_table])
            query = f"SELECT * FROM {selected_table} LIMIT 5"
            results = chatdb.execute_query(query)
            if results:
                print("Query Results:")
                for row in results:
                    print(row)
            else:
                print("No results found.")

        elif operation == 2:
            query = generator.generate_random_or_example_query(selected_table)
            print(query)
            print(chatdb.explain_query(selected_table, query))
            results = chatdb.execute_query(query)

            if results:
                print("Query Results:")
                for row in results:
                    print(row)
            else:
                print("No results found.")

        elif operation == 3:
            example = input('Please enter the SQL example you want: WHERE, GROUP BY, ORDER BY, LIMIT, BETWEEN').upper().strip()
            if example not in operations:
                print(f"{example} not in keyword operations, please enter a valid one")
                print("a random SQL example are generated")
                example = None

            query = generator.generate_random_or_example_query(selected_table, example)
            print(query)
            print(chatdb.explain_query(selected_table, query))
            results = chatdb.execute_query(query)

            if results:
                print("Query Results:")
                for row in results:
                    print(row)
            else:
                print("No Result Found.")

        elif operation == 4:
            sentence = input("please enter a sentence here:")
            sentence = sentence.replace(" ,", ",")
            sentence = sentence.replace(", and", ",").replace(",and", ",")
            original = sentence
            sentence = sentence.replace("  ", " ")
            sentence = sentence.replace(" and ", ",").replace(" group", "").replace(" in", "").replace(" of", "").replace(
            " the", "").replace(" from", "").replace(' table', "").replace("where", ",").replace(" or ", ",").replace(" ,", ",").replace(
            "_", " ")

            if 'by' not in sentence:
                sentence += " by ___"

            query = generator.generate_nlp_query(selected_table, sentence, original)
            replace_list = ["  ", "   "]
            for old in replace_list:
                query = query.replace(old, " ")

            query.replace(" ,", ", ")

            print(query)
            print(chatdb.explain_query(selected_table, query))
            results = chatdb.execute_query(query)

            if results:
                print("Query Results:")
                for row in results:
                    print(row)
            else:
                print("No Result Found.")

    # Close the database connection
    chatdb.close_connection()

