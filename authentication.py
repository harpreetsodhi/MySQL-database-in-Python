import json
import os

class Authentication:
    def __init__(self):
        self.users_list = None

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
        print('Not an authorised user!')
        return None

    # authenticates the entering user and shifts control to parser
    def verify_user(self):
        print("Enter User Name")
        user_name = input(">>")
        if self.users_list:
            for users in self.users_list:
                for key, value in users.items():
                    if users['name'] == user_name:
                        print('Enter password:')
                        user_pwd = input('>>')
                        if users['pass'] == user_pwd:
                            # call parser and pass user_name
                            print("Password is valid!")
                            return                         
                        else:
                            print('Password is not valid!')
            print("User name is not valid")
        else:
            print("No existing users to load!")

    def add_schema_to_user(self, user_name, schema_name, access):
        if self.users_list:
            for users in self.users_list:
                for key, value in users.items():
                    if users['name'] == user_name:
                        users['schemas'][schema_name] = access
        json.dump(self.users_list, open(os.path.join("user_schema.json"), "w+"), indent = 2)
        
def main():
    a = Authentication()
    a.load_users()
    # a.verify_user()
    a.add_schema_to_user("harpreet","test",[1,2,3])

if __name__ == "__main__":
    main()
