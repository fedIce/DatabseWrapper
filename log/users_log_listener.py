from core.Observer import event
from log.log import log_event

def user_created_log(data):
    log_event(f"A new user account with id {data['id']} was created for {data[u'display_name']} with email {data[u'email']}")

def user_updated_log(data):
    log_event(f"user account with id {data['id']} updated their account => {data}")

def user_deleted_log(data):
    log_event(f"user account with id {data['id']} was deleted")

def user_error_log(data):
    log_event(f"user encountered an error : ERROR ({data['error']})")



def setup_users_log_event_handlers():
    event.subscribe("user_created_log_event", user_created_log)
    event.subscribe("user_updated_log_event", user_updated_log)
    event.subscribe("user_deleted_log_event", user_deleted_log)
    event.subscribe("user_error_log_event", user_error_log)