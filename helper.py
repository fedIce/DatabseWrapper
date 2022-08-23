from typing import Any
import json

from core.pay_stack.errors import InvalidDataError


def extract_value(data: dict[str, Any], index: str):
    return data[f'{index}'] if f'{index}' in data else None 

def validate_missing_keys(data: dict[str, Any], keys: list):
    missing_keys = []
    for key in keys:
        if key not in data.keys():
            missing_keys.append(key)

    if len(missing_keys) > 0:
        raise InvalidDataError(f"Object is missing keys {missing_keys}")
    else:
        return data


def current_timestamp():
    # current date and time
    now = datetime.now()

    timestamp = datetime.timestamp(now)
    return timestamp

def serialize_objects(objects):
    '''
        inputs => list | dict
        json dumps lists and dictionaries in dict
    '''
    if type(objects) == list:
        for indx in range(0, len(objects)):
            for obj in objects[indx].keys():
                if type(objects[indx][obj]) in [list , dict]: 
                    objects[indx][obj] = json.dumps(objects[indx][obj])
        return objects
    else:
        for obj in objects.keys():
            if type(objects[obj]) in [list , dict]: 
                objects[obj] = json.dumps(objects[obj])
        return objects

def parse_db_results(field, input):
    data = []
    if(type(field) == str):
        key_list = field.split(',')
    else:
        key_list = field
    
    print(key_list, input)

    for result in input:
        obj = {}
        for key in range(0, len(key_list)):
            obj[key_list[key]] = result[key]
        data.append(obj)

    return data


def parse_where_clause(clause):
    and_clause = ['&','&&','and']
    or_clause = ['|','||','or']
    if clause.lower() in and_clause:
        return 'AND'
    elif clause.lower() in or_clause:
        return 'OR'
    else:
        return ''
