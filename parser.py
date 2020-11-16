import re
import os
import json


class Database:
    def __init__(self, location):
        self.user_name = "harpreet"
        self.user_access = ["C", "R", "U", "D"]
        self.schema = None
        self.schemas_directory = location
        self.create_schema_pattern = re.compile(r"create (database|schema)\s+(\w+)\s*;?$", re.IGNORECASE)
        self.use_schema_pattern = re.compile(r"use\s+(\w+)\s*;?$", re.IGNORECASE)
        self.create_table_pattern = re.compile(r"create table\s+(\w+)\s+\((.*)\)\s*;?$", re.IGNORECASE)

    def parse_query(self, query):
        # create schema query
        if self.create_schema_pattern.search(query):
            self.create_schema(self.create_schema_pattern.search(query).group(2))

        # use schema query
        elif self.use_schema_pattern.search(query):
            self.use_schema(self.use_schema_pattern.search(query).group(1))

        # create table query
        elif self.create_table_pattern.search(query):
            table_name = self.create_table_pattern.search(query).group(1)
            columns = self.create_table_pattern.search(query).group(2)
            columns = columns.split(",")
            columns = list(map(lambda x: x.strip(), columns))
            self.create_table(table_name, columns)

        # incorrect query
        else:
            print_error()

    def create_schema(self, schema_name):
        if schema_name in os.listdir(self.schemas_directory):
            print("Error! Schema already exists.")
        else:
            json.dump({}, open(os.path.join(self.schemas_directory, schema_name), "w+"))
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
            #
            self.schema = json.load(open(os.path.join(self.schemas_directory, schema_name), "r"))
            print("Current Schema:", schema_name)
        else:
            print("No such schema exists!")

    def drop_schema(self):
        pass

    def create_table(self, table_name, columns):
        if self.schema is None:
            print("Select a schema first")
            return
        if "C" not in self.user_access:
            print("You don't have write access to this schema")
            return

    def select_table(self):
        if self.schema is None:
            print("Select a schema first")
            return
        if "R" not in self.user_access:
            print("You don't have read access to this schema")
            return

    def update_table_name(self):
        if self.schema is None:
            print("Select a schema first")
            return
        if "U" not in self.user_access:
            print("You don't have update access to this schema")
            return

    def delete_table(self):
        if self.schema is None:
            print("Select a schema first")
            return
        if "D" not in self.user_access:
            print("You don't have delete access to this schema")
            return

    def create_row(self):
        if self.schema is None:
            print("Select a schema first")
            return
        if "C" not in self.user_access:
            print("You don't have write access to this schema")
            return

    def select_rows(self):
        if self.schema is None:
            print("Select a schema first")
            return
        if "R" not in self.user_access:
            print("You don't have read access to this schema")
            return

    def update_rows(self):
        if self.schema is None:
            print("Select a schema first")
            return
        if "U" not in self.user_access:
            print("You don't have update access to this schema")
            return

    def delete_rows(self):
        if self.schema is None:
            print("Select a schema first")
            return
        if "D" not in self.user_access:
            print("You don't have delete access to this schema")
            return


def get_path(schema_name):
    if not os.path.exists(schema_name):
        os.mkdir(schema_name)
    return os.path.join(os.getcwd(), schema_name)


def print_error():
    print("Invalid SQL syntax! Try again.")


def main():
    database = Database(get_path("schemas"))
    print("Welcome to DBMS")
    while True:
        query = input(">>")
        database.parse_query(query)


if __name__ == "__main__":
    main()