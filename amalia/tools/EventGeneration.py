import logging
import random
import string
import time
from datetime import datetime

logger = logging.getLogger(__name__.split('.')[-1])


def convert_date(date):
    if isinstance(date, str):
        return time.mktime(datetime.strptime(date, '%Y-%m-%d %H:%M:%S').timetuple())
    else:
        return date


def generate_random_time(current_day_time):
    hours = random.random() % 24
    minutes = random.random() % 60
    seconds = random.random() % 60
    return current_day_time + (hours * 60 + minutes) * 60 + seconds


def generate_random_node_id(k=22):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=k))