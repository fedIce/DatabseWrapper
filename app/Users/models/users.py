from datetime import datetime
from core.Database.data_types import GeoPointType, IntegerType, StringType, UUIDType, BOOLType, TimeStampType
from core.Database.interface import DatabaseInterface
import json
from helper import serialize_objects
from core.Observer.event import post_event

max_size_attribute = (10 * 1024 * 1024)


class User(DatabaseInterface):
    def __init__(self, email, phone_number, display_name, photo_url, email_verified=False, disabled=True, attributes={}, reviews=[], saved_posts=[]):
        self.display_name = display_name
        self.email = email
        self.email_verified = email_verified
        self.phone_number = phone_number
        self.photo_url = photo_url
        self.disabled = disabled
        self.created_at = datetime.now().isoformat(timespec='minutes')
        self.accounts = []
        self.reviews = []
        self.saved_posts = []
        self.attributes = attributes
        self.db = DatabaseInterface()
        self.keys = ['id', 'display_name', 'email', 'email_verified', 'phone_number',
                     'photo_url', 'disabled', 'created_at', 'accounts','reviews','saved_posts', 'attributes']

    @staticmethod
    def from_dict(source):

        if source is None:
            return {}

        user = User(source[u'email'], source[u'phone_number'],
                    source[u'display_name'], source[u'photo_url'])

        if u'email_verified' in source:
            user.phone_number = source[u'email_verified']

        if u'disabled' in source:
            user.disabled = source[u'disabled']

        if u'accounts' in source:
            user.accounts = source[u'accounts']

        if u'attributes' in source:
            user.attributes = source[u'attributes']

        return user

    def from_tuple(self, user):
        db = self.db
        key_list = self.keys
        obj = {}
        for key in range(0, len(key_list)):
            obj[key_list[key]] = user[key]

        obj['accounts'] = json.loads(obj['accounts'])
        obj['attributes'] = json.loads(obj['attributes'])
        return obj

    def to_dict(self):
        user = {
            u'email': self.email,
            u'phone_number': self.phone_number,
            u'display_name': self.display_name,
            u'photo_url': self.photo_url,
            u'email_verified': self.email_verified,
            u'disabled': self.disabled,
            u'created_at': self.created_at,
            u'accounts': self.accounts,
            u'reviews': self.reviews,
            u'saved_posts': self.saved_posts,
            u'attributes': self.attributes
        }

        return user

    def __repr__(self):
        return (
            f'City(\
                display_name = {self.display_name}, \
                email = {self.email}, \
                email_verified = {self.email_verified}, \
                phone_number = {self.phone_number}, \
                photo_url = {self.photo_url}, \
                disabled = {self.disabled}, \
                created_at = {self.created_at}, \
                accounts = {self.accounts}, \
                attributes = {self.attributes} \
            )'
        )

    def get_users_table_structure(self):
        return [
            {
                'key': 'id',
                'type': UUIDType(not_null=True, default=True)
            },
            {
                'key': 'display_name',
                'type': StringType(length=50, not_null=True)
            },
            {
                'key': 'email',
                'type': StringType(length=50, not_null=True)
            },
            {
                'key': 'email_verified',
                'type': BOOLType()
            },
            {
                'key': 'phone_number',
                'type': StringType(length=50, not_null=True)
            },
            {
                'key': 'photo_url',
                'type': StringType(length=150, not_null=True)
            },
            {
                'key': 'disabled',
                'type': BOOLType()
            },
            {
                'key': 'created_at',
                'type': TimeStampType()
            },
            {
                'key': 'accounts',
                'type': StringType(length=max_size_attribute)
            },
            {
                'key': 'reviews',
                'type': StringType(length=max_size_attribute)
            },
            {
                'key': 'saved_posts',
                'type': StringType(length=max_size_attribute)
            },
            {
                'key': 'attributes',
                'type': StringType(length=max_size_attribute)
            }
        ]

    def initialize_user_table(self, table: str = None):
        try:
            self.db.select_table(table=table)
            self.db.add_table_to_database(table=table)
            self.db.initialize_table_with_fields(
                fields=self.get_users_table_structure())
            return True
        except Exception as e:
            print(f'ERROR INITIALIZING USER TABLE: {e}')

    def add_user(self):
        try:
            user = self.to_dict()
            user = serialize_objects(user)
            new_user = self.db.add_record_to_table(records=[user])
            user['id'] = new_user
            post_event('user_created_log_event', user)

        except Exception as e:
            print(f'ERROR ADDING USER TABLE: {e}')
            post_event(event_type='user_error_log_event', data={'error':e})

    def get_users(self, where=None):
        users = self.db.select_table_field(', '.join(self.keys), where=where)
        return users

    def update_user(self, data:list=None):
        users = self.db.update_record(data=data, fields=', '.join(self.keys))
        post_event('user_updated_log_event', {'id':users[0]['id'], 'keys':data[0].keys()})
        return users

    def delete_user(self, key:str=None, value:str=None):
        id = self.db.delete_record(field={"key":key, "value": value})
        post_event('user_deleted_log_event', {'id':id})
        return id

    def add_user_property(self, column:list[dict]):
        return self.db.add_column(column)