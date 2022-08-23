from core.Observer import event
from log.log import log_event


def payment_successful_log(data):
    log_event(f"payment of {data['amount']}, from user with email {data['email']} successful")

def payment_initiated_log(data):
    log_event(f"payment of {data['amount']}, from user with email {data['email']} initiated")

def payment_failed_log(data):
    log_event(f"payment of {data['amount']}, from user with email {data['email']} failed")

def setup_transaction_log_event_handlers():
    event.subscribe("payment_successful_log_event", payment_successful_log)
    event.subscribe("payment_initialized_log_event", payment_initiated_log)
    event.subscribe("payment_failed_log_event", payment_failed_log)