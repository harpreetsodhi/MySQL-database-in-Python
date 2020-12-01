import json
import os
import parser_queries
import loggerprogram

class Authentication:
    def __init__(self):
        self.users_list = None
        self.load_users()

    # loads the user_schema file
    def load_users(self):
        with open("user_schema.json", encoding='utf-8', errors='ignore') as json_data:
            self.users_list = json.load(json_data, strict=False)

    # verifies if the user is present and verifies the password
    def check_access(self, user_name, schema_name):
        self.load_users()
        if self.users_list:
            for users in self.users_list:
                for key, value in users.items():
                    if schema_name in users['schemas']:
                        return users['schemas'][schema_name]
        return None

    # authenticates the entering user and shifts control to parser
    def verify_user(self):
        if self.users_list:
            while True:
                print("Enter User Name")
                user_name = input(">>")          
                for users in self.users_list:
                    if users['name'] == user_name:                       
                        while True:
                            print('Enter password:')
                            user_pwd = input('>>')
                            if users['pass'] == user_pwd:
                                print("User Authenticated")
                                loggerprogram.main(user_name)
                                parser_queries.main(user_name)
                                return                         
                            else:
                                print('Password is not valid!')
                print("Authentication error!")
        else:
            print("No existing users to load!")

    def add_schema_to_user(self, user_name, schema_name, access):
        if self.users_list:
            for users in self.users_list:
                if users['name'] == user_name:
                    users['schemas'][schema_name] = access
        json.dump(self.users_list, open(os.path.join("user_schema.json"), "w+"), indent=2)


def main():
    a = Authentication()
    a.verify_user()

if __name__ == "__main__":
    main()
