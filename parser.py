import re
import os
import json


class Database:
    def __init__(self, location):
        self.user_name = "harpreet"
        self.user_access = ["C", "R", "U", "D"]
        self.schema = None
        self.schema_name = ""
        self.schemas_directory = location
        self.create_schema_pattern = re.compile(r"^\s*create\s+(\s*database|schema\s*)\s+(\w+)\s*;?\s*$", re.IGNORECASE)
        self.use_schema_pattern = re.compile(r"^\s*use\s+(\w+)\s*;?\s*$", re.IGNORECASE)
        self.create_table_pattern = re.compile(r"^\s*create\s+table\s+(\w+)\s+\((.*)\)\s*;?\s*$", re.IGNORECASE)
        self.column_pattern = re.compile(r"(\w+)\s+(int|varchar\s*\(\s*\d+\s*\)|decimal\s*\(\s*\d+\s*@\s*\d+\s*\))"
                                         r"(\s+not\s+null)?(\s+primary\s+key)?$", re.IGNORECASE)
        self.select_table_pattern = re.compile(r"^\s*select\s+(.*)\s+from\s+(\w+)(\s+where\s+.*)?\s*;?\s*$", re.IGNORECASE)
        self.update_table_pattern = re.compile(r"^\s*update\s+(\w+)\s+set(.*)\s*$", re.IGNORECASE)
        self.drop_table_pattern = re.compile(r"^\s*drop\s+table\s+(\w+)\s*;?\s*$", re.IGNORECASE)
        self.drop_schema_pattern = re.compile(r"^\s*drop\s+database\s+(\w+)\s*;?\s*$", re.IGNORECASE)
        self.insert_row_pattern = re.compile(r"^\s*insert\s+into\s+(\w+)\s+values\s*\((.*)\)\s*;?\s*$", re.IGNORECASE)
        self.delete_rows_pattern = re.compile(r"^\s*delete\s+from\s+(\w+)(\s+where\s+.*)?\s*;?\s*$", re.IGNORECASE)

    def parse_query(self, query):
        # create schema
        if self.create_schema_pattern.search(query):
            self.create_schema(self.create_schema_pattern.search(query).group(2))

        # use schema
        elif self.use_schema_pattern.search(query):
            self.use_schema(self.use_schema_pattern.search(query).group(1))

        # create table
        elif self.create_table_pattern.search(query):
            table_name = self.create_table_pattern.search(query).group(1)
            columns = self.create_table_pattern.search(query).group(2)

            # change "," in decimal field to "@"
            decimal_field = re.search("decimal\s*\(\s*\d+\s*(,)\s*\d+\s*\)", columns)
            if decimal_field:
                comma_index = decimal_field.span(1)[0]
                columns = columns[:comma_index] + "@" + columns[comma_index + 1:]

            columns = columns.split(",")
            columns = list(map(lambda x: x.strip(), columns))
            self.create_table(table_name, columns)

        # select table
        elif self.select_table_pattern.search(query):
            columns = self.select_table_pattern.search(query).group(1)
            table_name = self.select_table_pattern.search(query).group(2)
            where_condition = self.select_table_pattern.search(query).group(3)
            if where_condition:
                where_condition = clean_where(where_condition)
            columns = columns.split(",")
            columns = list(map(lambda x: x.strip(), columns))
            self.select_table(table_name, columns, where_condition)

        # update table
        elif self.update_table_pattern.search(query):
            table_name = self.update_table_pattern.search(query).group(1)
            values = self.update_table_pattern.search(query).group(2)
            where_condition = re.search(r"where\s+.*", values, flags=re.IGNORECASE)
            if where_condition:
                where_condition = clean_where(where_condition.group())
            values = re.sub(r"where\s+.*", "", values, flags=re.IGNORECASE)
            self.update_table(table_name, values.strip(), where_condition)

        # drop table
        elif self.drop_table_pattern.search(query):
            table_name = self.drop_table_pattern.search(query).group(1)
            self.drop_table(table_name)

        # drop schema
        elif self.drop_schema_pattern.search(query):
            schema_name = self.drop_schema_pattern.search(query).group(1)
            self.drop_schema(schema_name)

        # insert row
        elif self.insert_row_pattern.search(query):
            table_name = self.insert_row_pattern.search(query).group(1)
            values = self.insert_row_pattern.search(query).group(2)
            values = values.split(",")
            values = list(map(lambda x: x.strip(), values))

            for i in range(len(values)):
                if values[i].isdigit():
                    values[i] = int(values[i])
                else:
                    try:
                        values[i] = float(values[i])
                    except ValueError:
                        values[i] = re.sub("'", "", values[i])
                        values[i] = re.sub('"', '', values[i])
            self.insert_row(table_name, values)

        # delete rows
        elif self.delete_rows_pattern.search(query):
            table_name = self.delete_rows_pattern.search(query).group(1)
            where_condition = self.delete_rows_pattern.search(query).group(2)
            if where_condition:
                where_condition = clean_where(where_condition)
            self.delete_rows(table_name, where_condition)

        # incorrect query
        else:
            print("Invalid SQL syntax! Try again.")

    def create_schema(self, schema_name):
        if schema_name in os.listdir(self.schemas_directory):
            print("Error! Schema already exists.")
        else:
            json.dump({}, open(os.path.join(self.schemas_directory, schema_name), "w+"), indent=2)
            # call a method to add the schema name to the user file with full access
            # add_schema_to_user(self.user_name, schema_name, ["C", "R", "U", "D"])
            print("Schema created with name:", schema_name)

    def use_schema(self, schema_name):
        if schema_name in os.listdir(self.schemas_directory):
            # call a method and send user_name and schema_name to check for access
            # access_level = check_access(self.user_name, self.schema_name):
            #   if access_level is None:
            #       print("You don't have access to this schema")
            #   else:
            #       self.user_access = access_level
            self.schema = json.load(open(os.path.join(self.schemas_directory, schema_name), "r"))
            self.schema_name = schema_name
            print("Current Schema:", self.schema_name)
        else:
            print("No such schema exists!")

    def drop_schema(self, schema_name):
        if schema_name in os.listdir(self.schemas_directory):
            os.remove(os.path.join(self.schemas_directory, schema_name))
            print("Dropped", schema_name)
        else:
            print("No such schema exists!")

    def create_table(self, table_name, columns):
        if self.schema is None:
            print("Select a schema first")
            return
        if "C" not in self.user_access:
            print("You don't have write access to this schema")
            return
        if table_name in self.schema.keys():
            print("Table name already exists")
            return
        values = [{}]
        for column in columns:
            if self.column_pattern.search(column):
                pk = 0
                nn = 0
                column_name = self.column_pattern.search(column).group(1).strip()
                column_type = self.column_pattern.search(column).group(2).strip()
                if "int" in column_type.lower():
                    column_type = "int"
                elif "varchar" in column_type.lower():
                    column_type = "str"
                elif "decimal" in column_type.lower():
                    column_type = "float"
                if self.column_pattern.search(column).group(3):
                    nn = 1
                if self.column_pattern.search(column).group(4):
                    pk = 1
                meta_data = {"PK": pk, "NN": nn, "type": column_type}
                values[0][column_name] = meta_data
            else:
                print("Invalid Syntax! Please refer the sql syntax for creating table.")
                return
        self.schema[table_name] = {}
        self.schema[table_name]["values"] = values
        json.dump(self.schema, open(os.path.join(self.schemas_directory, self.schema_name), "w+"), indent=2)
        print("created table:", table_name)

    def select_table(self, table_name, columns, where_condition):
        if self.schema is None:
            print("Select a schema first")
            return
        if "R" not in self.user_access:
            print("You don't have read access to this schema")
            return
        if table_name not in self.schema.keys():
            print("Table does not exist")
            return
        meta_data = self.schema[table_name]["values"][0]
        for column in columns:
            if column == "*":
                continue
            else:
                if column not in meta_data.keys():
                    print("Column Error! Column name does not exist")
                    return
        rows = self.schema[table_name]["values"][1:]
        if where_condition:
            try:
                rows = list(filter(lambda row: eval(f"{where_condition}", locals(), row), rows))
            except:
                print("Error! Invalid where clause")
                return
        print("Number of rows:", len(rows))
        if "*" in columns:
            print(json.dumps(rows, indent=2))
            return
        print(json.dumps([{i: row[i] for i in columns} for row in rows], indent=2))

    def update_table(self, table_name, update_condition, where_condition):
        if self.schema is None:
            print("Select a schema first")
            return
        if "U" not in self.user_access:
            print("You don't have update access to this schema")
            return
        if table_name not in self.schema.keys():
            print("Table does not exist")
            return
        rows = self.schema[table_name]["values"][1:]
        if where_condition:
            try:
                rows = list(filter(lambda row: eval(f"{where_condition}", locals(), row), rows))
            except:
                print("Error! Invalid where clause")
                return
        try:
            map(lambda row: exec(f"{update_condition}", locals(), row), rows)
        except:
            print("Error! Invalid update parameters")
            return
        rows_count = len(rows)
        rows = self.schema[table_name]["values"][1:]

        where_filter = lambda row: eval(f"{where_condition}", locals(), row)
        updated_rows = lambda row: exec(f"{update_condition}", locals(), row)

        if where_condition:
            for row in rows:
                if where_filter(row):
                    updated_rows(row)
        else:
            for row in rows:
                updated_rows(row)

        json.dump(self.schema, open(os.path.join(self.schemas_directory, self.schema_name), "w+"), indent=2)
        print("Number of rows updated:", rows_count)

    def drop_table(self, table_name):
        if self.schema is None:
            print("Select a schema first")
            return
        if "D" not in self.user_access:
            print("You don't have delete access to this schema")
            return
        if table_name not in self.schema.keys():
            print("Table does not exist")
            return
        self.schema.pop(table_name, None)
        json.dump(self.schema, open(os.path.join(self.schemas_directory, self.schema_name), "w+"), indent=2)
        print("dropped", table_name)

    def insert_row(self, table_name, values):
        if self.schema is None:
            print("Select a schema first")
            return
        if "C" not in self.user_access:
            print("You don't have write access to this schema")
            return
        if table_name not in self.schema.keys():
            print("Table does not exist")
            return
        column_names = list(self.schema[table_name]['values'][0].keys())
        no_of_columns = len(column_names)
        if len(values) != no_of_columns:
            print("Invalid Syntax! Please refer the sql syntax for inserting rows.")
            return
        row = {}
        for i in range(no_of_columns):
            row[column_names[i]] = values[i]
        self.schema[table_name]["values"].append(row)
        print("1 row inserted")
        json.dump(self.schema, open(os.path.join(self.schemas_directory, self.schema_name), "w+"), indent=2)

    def delete_rows(self, table_name, where_condition):
        if self.schema is None:
            print("Select a schema first")
            return
        if "D" not in self.user_access:
            print("You don't have delete access to this schema")
            return
        if table_name not in self.schema.keys():
            print("Table does not exist")
            return
        rows = self.schema[table_name]["values"][1:]
        filtered_rows = []
        if where_condition:
            try:
                filtered_rows = list(filter(lambda row: eval(f"{where_condition}", locals(), row), rows))
            except:
                print("Error! Invalid where clause")
                return
        else:
            self.schema[table_name]["values"] = self.schema[table_name]["values"][0:1]
            json.dump(self.schema, open(os.path.join(self.schemas_directory, self.schema_name), "w+"), indent=2)
            print(len(rows), "rows deleted")
            return
        self.schema[table_name]["values"][1:] = [row for row in rows if row not in filtered_rows]
        json.dump(self.schema, open(os.path.join(self.schemas_directory, self.schema_name), "w+"), indent=2)
        print(len(filtered_rows), "rows deleted")


def get_path(schema_name):
    if not os.path.exists(schema_name):
        os.mkdir(schema_name)
    return os.path.join(os.getcwd(), schema_name)


def clean_where(where_condition):
    where_condition = re.sub("where ", "", where_condition, flags=re.IGNORECASE)
    where_condition = re.sub(";", "", where_condition, flags=re.IGNORECASE)
    where_condition = re.sub("=", "==", where_condition, flags=re.IGNORECASE)
    where_condition = re.sub(" or ", " or ", where_condition, flags=re.IGNORECASE)
    where_condition = re.sub(" and ", " and ", where_condition, flags=re.IGNORECASE)
    return where_condition


def main():
    database = Database(get_path("schemas"))
    print("Welcome to DBMS")
    while True:
        query = input(">>")
        database.parse_query(query)


if __name__ == "__main__":
    main()
