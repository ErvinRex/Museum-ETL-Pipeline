"""
Script to take in raw data and filter for invalid messages
"""

import logging
from datetime import datetime, time
import json

VALID_TYPES = [0, 1]
VALID_VALS = [-1, 0, 1, 2, 3, 4]
VALID_SITES = ["0", "1", "2", "3", "4", "5"]
VALID_AT_LEN = 32
OPENING_HOUR = time(8, 45)
CLOSING_HOUR = time(18, 15)


def clean_data(message):
    """Cleans the Kiosk data from the museum"""

    data = message.value().decode()
    json_data = json.loads(message.value().decode())

    miss_check = missing_values(data, json_data)
    if miss_check:
        inv_check = invalid_values(data, json_data)
        if inv_check:
            return True
    return False


def invalid_values(data, json_data):
    """Check for invalid values in message"""

    if json_data['val'] not in VALID_VALS:
        logging.error(
            f"Invalid 'val' key value: {data}. 'val' must be one of {VALID_VALS}")
        return False

    if json_data['site'] not in VALID_SITES:
        logging.error(
            f"Invalid 'site' key value: {data}. 'site' must be one of {VALID_SITES}")
        return False

    if 'type' in json_data and json_data['type'] not in VALID_TYPES:
        logging.error(
            f"Invalid 'type' key value: {data}. 'type' must be one of {VALID_TYPES}")
        return False

    if not json_data['at'] or len(json_data['at']) != VALID_AT_LEN:
        logging.error(f"Invalid 'at' key value: {data}")
        return False

    try:
        datetime.strptime(json_data['at'], '%Y-%m-%dT%H:%M:%S.%f+00:00')
    except:
        logging.error(f"Invalid 'at' date format: {data}")
        return False

    at_value = datetime.strptime(
        json_data['at'], '%Y-%m-%dT%H:%M:%S.%f+00:00').time()
    if not OPENING_HOUR <= at_value <= CLOSING_HOUR:
        logging.error(
            f"Invalid 'at' time value: {data}. Time can only be between 08:45 and 18:15")
        return False

    return True


def missing_values(data, json_data):
    """Check for missing values in message"""

    if "at" not in json_data:
        logging.error(f"Missing 'at' key: {data}")
        return False

    if "val" not in json_data:
        logging.error(f"Missing 'val' key: {data}")
        return False

    if json_data['val'] == -1 and "type" not in json_data:
        logging.error(f"Missing 'type' key for support instance: {data}")
        return False

    if "site" not in json_data:
        logging.error(f"Missing 'site' key: {data}")
        return False

    if not json_data['val'] and json_data['val'] != 0:
        logging.error(f"Missing 'val' key value: {data}")
        return False

    if not json_data['site'] and json_data['site'] != 0:
        logging.error(f"Missing 'site' key value: {data}")
        return False

    return True
