from app.Users.models.users import User

user = User(email="mailer@mailer.com", phone_number="+90533839920", display_name="Mark Zuckerberg", photo_url="https://q-xx.bstatic.com/xdata/images/hotel/max1024x768/306933569.jpg?k=726d2bfed904896d6b36e9beca59fa7306d0bb01cb0b4544774a4278859e0a89&o=")

# user.initialize_user_table(table="users")
user.select_table(table="users")
user.add_user()
user.get_users()

users = user.get_users(where=[ {'key':'phone_number', 'condition':'=', 'value':"'+90533839920'", "clause":"or"},{'key':'email', 'condition':'=', 'value':"'mailer@mailer.com'"}])


print('DELETED: => ',user.delete_user(key="id", value="'5f6d8177-7841-4748-bf85-2cce217e9760'"))
user.add_user_property(column= [
            {
                'key': 'firstname',
                'type': StringType(length=50)
            },
            {
                'key': 'lastname',
                'type': StringType(length=50)
            },
            {
                'key': 'age',
                'type': IntegerType()
            },
        ])

user.db.commit()
