import json


class Authentication:
    def __init__(self):
        self.users_list = None

    # loads the user_schema file
    def load_users(self):
        with open("user_schema.json", encoding='utf-8', errors='ignore') as json_data:
            self.users_list = json.load(json_data, strict=False)

    # verifies if the user is present and verifies the password
    def verify_user(self, user_name, schema_name):
        self.load_users()
        if self.users_list:
            for users in self.users_list:
                for key, value in users.items():
                    if users['name'] == user_name:
                        print('Enter the password:')
                        user_pwd = input('>>')
                        if users['pass'] == user_pwd:
                            if schema_name in users['schemas']:
                                return users['schemas'][schema_name]
                        else:
                            print('Password is not valid!')
                        return
        print('Not an authorised user!')
        return

# def main():
#     a = Authentication()
#     print(a.verify_user('harpreet','schema2'))

# if __name__ == "__main__":
#     main()
