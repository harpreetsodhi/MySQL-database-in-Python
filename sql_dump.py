import os
import re

class SQL_dump:
    def __init__(self):
        self.files = os.listdir(os.path.join(os.getcwd(), "schemas"))
    
    def structure_dump(self):
        with open(os.path.join("output.txt"), "w+") as out_obj:
            for file in self.files:
                with open(os.path.join("schemas", file), "r") as f_obj:
                    filedata = eval(f_obj.read())
                    out_obj.write("CREATE DATABASE " +file.split(".")[0]+"\n")
                    for table in filedata.keys():            
                        create_table_query="CREATE TABLE "+table+" ("
                        column_list=list(filedata[table]["values"][0].keys())
                        for key in column_list:
                            if filedata[table]["values"][0][key]["type"]=="int":
                                create_table_query+=key+" int,"
                            elif filedata[table]["values"][0][key]["type"]=="str":
                                create_table_query+=key+" varchar(255),"
                            elif filedata[table]["values"][0][key]["type"]=="float":
                                create_table_query+=key+" decimal(10,5),"
                        create_table_query=create_table_query[:-1]+");\n"
                        out_obj.write(create_table_query)             

def main():
    a = SQL_dump()
    a.structure_dump()

if __name__ == "__main__":
    main()