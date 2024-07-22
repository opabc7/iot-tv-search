#!/usr/bin/env python3

def get_value_with_default(json_data, key, dtype):
    if key in json_data:
        return json_data[key]
    else:
        if dtype is str:
            return ''
        elif dtype is int:
            return 0
        elif dtype is float:
            return 0.0
        elif dtype is dict:
            return {}
        elif dtype is list:
            return []
        elif dtype is bool:
            return False
        else:
            return None
