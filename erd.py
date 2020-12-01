import os


class ERD_Table:
    def __init__(self):
        self.files = os.listdir(os.path.join('schemas'))
        self.output = {}

    def schema_parser(self):
        for file in self.files:
            with open(os.path.join("schemas", file), "r") as f_obj:
                filedata = eval(f_obj.read())
                for keys in filedata:
                    if "FKcontraints" in filedata[keys].keys():
                        self.output[keys] = {}
                        for fk_keys in filedata[keys]["FKcontraints"]:
                            fk = filedata[keys]["FKcontraints"][fk_keys].split(".")[
                                1]
                            associated_table = filedata[keys]["FKcontraints"][fk_keys].split(".")[
                                0]
                            self.output[keys][associated_table] = [
                                [fk+"(1)"], [fk+"(FK)(1)"]]
                            self.output[keys][associated_table][0].extend(list(filedata[keys]
                                                                               ["values"][0].keys()))
                            self.output[keys][associated_table][0].remove(fk)
                            self.output[keys][associated_table][1].extend(list(filedata[associated_table]
                                                                               ["values"][0].keys()))
                            self.output[keys][associated_table][1].remove(fk)
                print(file)
                self.table_display()

    def table_display(self):
        for key in self.output:
            for a_table_key in self.output[key]:
                print("_" * 19)
                print("|"+""*15+"{:^17}|".format(key))
                print("|"+"_" * 17+"|")
                for i in self.output[key][a_table_key][0]:
                    print("|{:^17}|".format(i))
                print("|"+"_" * 17+"|\n")
                print("Has relationship with")
                print("_" * 19)
                print("|"+""*15+"{:^17}|".format(a_table_key))
                print("|"+"_" * 17+"|")
                for i in self.output[key][a_table_key][1]:
                    print("|{:^17}|".format(i))
                print("|"+"_" * 17+"|\n")
                print("-"*30)

def main():
    a = ERD_Table()
    a.schema_parser()
    # a.table_display()

if __name__ == "__main__":
    main()