import random
import re
import string
import pandas as pd
import nltk
from nltk.tokenize import word_tokenize

nltk.download('punkt')

class QueryGenerator:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.keywords = ["WHERE", "GROUP BY", "ORDER BY", "LIMIT", "BETWEEN"]
        self.Aggregation = ['SUM', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN']
        self.template = "SELECT <column_name> <function> FROM <table_name> "

    def get_tables_and_columns(self):
        """Retrieve tables and their columns with data types from the database."""
        with self.db_connection.cursor() as cursor:
            # Retrieve all table names
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]

            schema = {}
            # Retrieve columns and their data types for each table
            for table in tables:
                cursor.execute(f"DESCRIBE {table}")
                schema[table] = {row[0]: row[1] for row in cursor.fetchall()}  # {column_name: data_type}

            schema_data = {}
            for table, columns in schema.items():
                schema_data[table] = [f"{col_name}: {col_type}" for col_name, col_type in columns.items()]

            # Convert to DataFrame
            df_schema = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in schema_data.items()]))
            return schema, df_schema

    def generate_random_or_example_query(self, table, example=None):
        """Generate a random query based on templates and schema."""
        schema, _ = self.get_tables_and_columns()
        if not schema or table not in schema:
            return "No valid tables found in the database."

        columns = schema[table]

        valid_columns_for_aggregation = [
            col for col, dtype in columns.items()
            if dtype in ['int', 'float'] and 'id' not in col.lower()
        ]

        # Select random keywords
        selected_keywords = random.sample(self.keywords, random.randint(1, len(self.keywords)))

        if example is not None and example not in selected_keywords:
            selected_keywords.append(example)

        # Randomly choose an aggregation function if 'GROUP BY' is selected
        aggregation_function = None
        if "GROUP BY" in selected_keywords:
            if valid_columns_for_aggregation:
                aggregation_function = random.choice(self.Aggregation)
            else:
                # Skip GROUP BY if no valid numeric columns for aggregation
                selected_keywords.remove("GROUP BY")

        agg_col = random.choice(valid_columns_for_aggregation)

        agg_operation = "," + aggregation_function + f"({agg_col})" if aggregation_function is not None else ""

        # Randomly choose one or more columns
        selected_columns = random.sample(list(columns.keys()), random.randint(1, len(columns)))

        # Construct the query based on selected keywords
        query = self.template.replace("<column_name>", ", ".join(selected_columns)).replace("<function>", agg_operation).replace("<table_name>", table)

        # Handle WHERE logic (if applicable)
        if "WHERE" in selected_keywords or "BETWEEN" in selected_keywords:
            column = selected_columns[0]  # Just use the first selected column for the WHERE clause
            column_type = columns[column]

            if column_type in ['int', 'float']:  # Numeric column
                where_condition = f" WHERE {column} > {random.randint(0, 10)} AND {column} < {random.randint(11, 1500000)}"
                query += where_condition
            elif column_type == 'varchar(255)':  # String column
                random_char1 = random.choice(string.ascii_lowercase)
                random_char2 = random.choice(string.ascii_lowercase)
                query += f" WHERE {column} LIKE '%{random_char1}%' OR {column} LIKE '%{random_char2}%'"
            else:
                query += f" WHERE {column} IS NOT NULL"  # Default condition if column type is not handled

        # Handle GROUP BY logic (if applicable)
        if "GROUP BY" in selected_keywords:
            query += f" GROUP BY {', '.join(selected_columns)}"

        # Handle ORDER BY logic (if applicable)
        if "ORDER BY" in selected_keywords:
            query += f" ORDER BY {', '.join(selected_columns)} DESC"

        if "LIMIT" in selected_keywords:
            limit_number = random.randint(1, 10)  # Random limit number from 1 to 10
            query += f" LIMIT {limit_number}"

        query = query.lstrip("\ufeff")
        return query

    def generate_nlp_query(self, table, sentence, original):

        tokens = word_tokenize(sentence.lower())

        schema, _ = self.get_tables_and_columns()
        if not schema or table not in schema:
            return "No valid tables found in the database."

        columns = schema[table]
        columns_map = {key.replace(" ", "").replace("_", "").lower():key for key in schema[table].keys()}

        columns_map['id'] = 'id'

        valid_columns_for_aggregation = {
            col.replace(" ", "").replace("_", "").lower(): col
            for col, dtype in columns.items()
            if dtype in ['int', 'float']
        }

        valid_columns_for_aggregation['id'] = 'id'

        targets = self._find_target_columns(tokens)
        matched_columns = self.find_matched_columns(targets, columns_map)
        function = self.find_aggregation_function(sentence, columns_map, valid_columns_for_aggregation)
        group_by_columns = self.find_aggregation_columns(sentence, columns_map)
        where_condition = self.find_where_condition(sentence, columns_map, valid_columns_for_aggregation, original)

        if function:
            matched_columns = group_by_columns
            matched_columns += "," if matched_columns else ""

        query = self.template

        query += where_condition
        query += f" GROUP BY {group_by_columns}" if group_by_columns else ""

        query = query.replace("<column_name>", matched_columns).replace("<function>", function).replace("<table_name>", table)

        query = query.lstrip("\ufeff")

        query.replace("  ", " ").replace("   ", " ")
        return query

    def find_matched_columns(self, targets, columns_map):
        matched_columns = []
        if targets:
            # Split attributes by 'and' or commas
            attribute_list = targets.split(",")
            for attr in attribute_list:
                normalized_attr = self._remove_space(attr)

                if normalized_attr in columns_map:
                    matched_columns.append(columns_map[normalized_attr])
                elif 'id' in normalized_attr:
                    if 'id' not in matched_columns:
                        matched_columns.append('id')

        if not matched_columns:
            print("no columns are matched in the table, give you all columns instead")
            matched_columns.append("*")

        return ", ".join(matched_columns)

    def _find_target_columns(self, tokens):
        verbs = ['find', 'get', 'show', 'select', 'the']
        prepositions = ['of', 'from', 'in', 'where', 'by']
        attributes = []
        start_extracting = False
        for i, token in enumerate(tokens):
            if token in verbs and not start_extracting:
                start_extracting = True
                continue
            if start_extracting:
                if token in prepositions:
                    break
                attributes.append(token)
        return ''.join(attributes)

    def _remove_space(self, text):
        return text.lower().replace(' ', '').replace('_', '').lower()

    def find_aggregation_function(self, sentence, columns_map, valid_columns_for_aggregation):
        """
        Detect if the sentence has aggregation function.
        """
        operation_map = {
            "total": "SUM",
            "sum": "SUM",
            "summarize": "SUM",
            "avg": "AVG",
            "average": "AVG",
            "highest": "MAX",
            "largest": "MAX",
            "greatest": "MAX",
            "max": "MAX",
            "maximum": "MAX",
            "lowest": "MIN",
            "min": "MIN",
            "smallest": "MIN",
            "minimum": "MIN",
            "count": "COUNT",
            "number": "COUNT",
            "how many": "COUNT"
        }


        for keyword, operation in operation_map.items():
            index = sentence.find(keyword)
            if index > 0:
                index += len(keyword)
                col = ""
                while index < len(sentence):
                    if sentence[index] == ",":
                        break
                    if sentence[index:index+2] == 'by':
                        break

                    if sentence[index] == " ":
                        index += 1
                        continue

                    col += sentence[index]
                    index += 1

                if col not in columns_map:
                    print(f"this column {col} is not a column in the table")
                    return ""

                op = operation_map[keyword]
                if op == 'COUNT' or col in valid_columns_for_aggregation:
                    return f"{op}({columns_map[col]}) "
                else:
                     print(f"this column {col} is not a valid column for agg")
                     return ""

        return ""

    def find_aggregation_columns(self, sentence, columns_map):
        index = sentence.find("by") + 2
        column = ""
        while index < len(sentence):
            if sentence[index] == " ":
                index += 1
                continue

            column += sentence[index]
            index += 1

        cols = column.split(",")

        group_by = " "
        for col in cols:
            if col not in columns_map:
                if col != "___":
                    print(f"this column {col} is not a column in the table, ignore this column")
            else:
                group_by += f" {columns_map[col]},"

        group_by = group_by[:-1] if group_by[-1] == "," else group_by

        return group_by.strip() if group_by.strip() != " " else ""

    def find_where_condition(self, sentence, columns_map, valid_columns_for_aggregation, original):
        operator_map = {
            'above': '>',
            'under': '<',
            'equal': '=',
            'lik': 'LIKE',
        }

        results = []

        sentence = sentence.lower().replace('find', "").replace("show", "")
        greater_than = ['is greater than', 'is bigger than', 'is more than', 'is higher than', 'is larger than', 'bigger than', 'more than', 'higher than', 'larger than', 'exceeds', 'greater than', ">"]
        for value in greater_than:
            sentence = self._replace_where_condition(sentence, value, 'above')

        less_than = ['is less than','is lower than','is smaller than','lower than','smaller than','below','less than',"<"]

        for value in less_than:
            sentence = self._replace_where_condition(sentence, value, 'under')

        equals_to = ['is equal to', "is equal", 'equals to', 'equal to', 'equals', 'is', "="]

        for value in equals_to:
            sentence = self._replace_where_condition(sentence, value, 'equal')

        like = ['look like', 'with', 'have', 'like']
        for value in like:
            sentence = self._replace_where_condition(sentence, value, 'lik')

        if "by" in sentence:
            sentence = sentence.split("by")[0]

        for key, operator in operator_map.items():
            start_index = 0
            while key in sentence[start_index:]:
                start_index = sentence.find(key, start_index)
                end_index = start_index + len(key)

                before_key = sentence[:start_index].strip()
                before_key = before_key.split(",")[-1].strip().replace(" ", "")

                after_key = sentence[end_index:].strip()
                after_key_split = re.split(r',', after_key)

                if after_key_split:
                    after_key = after_key_split[0].strip()

                    start = sentence.find(after_key)
                    prev_char = sentence[start-2]

                    if prev_char == 'l':  # for equal
                        if after_key.isdigit():
                            after_key = '= ' + after_key
                        else:
                            after_key = "= " + f"'{after_key}'"
                    elif prev_char == 'e':  # for greater than
                        after_key = '> ' + after_key
                    elif prev_char == 'r':  # for less than
                        after_key = '< ' + after_key
                    else:
                        after_key = 'LIKE ' + f"'%{after_key}%'"

                    results.append((before_key, after_key))

                start_index = end_index

        where_condition = " WHERE "

        for i in range(len(results)):
            col = results[i-1][0] if not results[i][0] and i > 0 else results[i][0]

            if col not in columns_map:
                print(f"{col} column is not in the table, ignore this condition")
                continue

            if results[i][1][0] == ">" or results[i][1][0] == "<":
                if col not in valid_columns_for_aggregation:
                    print(f"the column {col} is not for > or < operations, ignore this condition")
                    continue

            where_condition += f"{columns_map[col]} {results[i][1]}"

            if i < len(results)-1:
                char = original.find(results[i][1][1:]) + len(results[i][1][1:])+1

                if char == 'a':
                    where_condition += " AND "
                elif char == 'o':
                    where_condition += " OR "
                else:
                    where_condition += " AND "

        if where_condition.strip().endswith("AND"):
            where_condition = where_condition[:-4].strip()
        elif where_condition.strip().endswith("OR"):
            where_condition = where_condition[:-2].strip()

        print(where_condition)
        if where_condition.strip() == "WHERE":
            return ""
        else:
            return where_condition

    def _replace_where_condition(self, sentence, value, changed_value):
        return sentence.replace(value, changed_value)














