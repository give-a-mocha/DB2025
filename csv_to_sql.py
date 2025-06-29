import csv
import os

def infer_type(value):
    """
    Infer data type from a string value.
    """
    if value is None or value.strip() == '':
        return 'CHAR(50)'  # Default to CHAR for empty values
    try:
        int(value)
        return 'INT'
    except ValueError:
        try:
            float(value)
            return 'FLOAT'
        except ValueError:
            return 'CHAR(50)'

def csv_to_sql(csv_file_path):
    """
    Convert a CSV file to a SQL file with CREATE TABLE and INSERT statements.
    """
    if not os.path.exists(csv_file_path):
        print(f"Error: File not found at {csv_file_path}")
        return

    table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    output_dir = "sql"
    os.makedirs(output_dir, exist_ok=True)
    sql_file_path = os.path.join(output_dir, f"{table_name}.sql")

    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file)
        
        try:
            header = next(reader)
            # Read the first data row to infer types
            first_data_row = next(reader)
        except StopIteration:
            print("CSV file is empty or contains only a header.")
            return

        column_types = [infer_type(value) for value in first_data_row]

        with open(sql_file_path, 'w', encoding='utf-8') as sql_file:
            # DROP TABLE statement
            sql_file.write(f"DROP TABLE {table_name};\n\n")

            # CREATE TABLE statement
            columns_def = []
            for i, col_name in enumerate(header):
                columns_def.append(f"{col_name.strip()} {column_types[i]}")
            create_table_statement = f"CREATE TABLE {table_name} ({', '.join(columns_def)});\n\n"
            sql_file.write(create_table_statement)

            # Function to write INSERT statements
            def write_insert_statement(row):
                values = []
                for i, value in enumerate(row):
                    if column_types[i] in ('INT', 'FLOAT'):
                        # Handle empty strings for numeric types as NULL
                        values.append(value if value.strip() != '' else 'NULL')
                    else:
                        # Escape single quotes for SQL strings
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                sql_file.write(f"INSERT INTO {table_name} VALUES ({', '.join(values)});\n")

            # Write the first data row's INSERT statement
            write_insert_statement(first_data_row)

            # Write the rest of the data rows
            for row in reader:
                write_insert_statement(row)
            sql_file.write(f"SELECT * FROM {table_name};\n")

    print(f"Successfully converted {csv_file_path} to {sql_file_path}")

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python csv_to_sql.py <path_to_csv_file_or_directory>")
        sys.exit(1)

    path = sys.argv[1]

    if os.path.isdir(path):
        for filename in os.listdir(path):
            if filename.endswith(".csv"):
                file_path = os.path.join(path, filename)
                csv_to_sql(file_path)
    elif os.path.isfile(path):
        csv_to_sql(path)
    else:
        print(f"Error: Path '{path}' is not a valid file or directory.")
        sys.exit(1)