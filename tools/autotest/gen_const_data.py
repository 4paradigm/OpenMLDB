import random
import time

import numpy as np

from fesql_const import current_time


def random_literal_bool(nullable=True):
    if nullable:
        return random.choice(["true", "false", "NULL"])
    else:
        return random.choice(["true", "false"])

def random_literal_int16():
    return random.randint(-2 ** 16, 2 ** 16 - 1)

def random_literal_int32():
    return random.randint(-2 ** 32, 2 ** 32 - 1)

def random_literal_int64():
    if random.randint(0, 1) == 0:
        return random.randint(2 ** 32, 2 ** 64 - 1)
    else:
        return random.randint(-2 ** 64, -2 ** 32 - 1)

def random_literal_timestamp():
    lower = 0
    # lower = int(time.mktime(time.strptime("2000-01-01", "%Y-%m-%d")))
    upper = int(current_time.timestamp()*1000)
    return random.randint(lower, upper)

def random_literal_date():
    res = random_literal_timestamp()
    res = time.strftime("%Y-%m-%d", time.localtime(res/1000))
    return res

def random_literal_float():
    lower = np.finfo(np.float32).min
    upper = np.finfo(np.float32).max
    # str(round(np.random.uniform(lower, upper), 5)) + "f"
    return np.random.uniform(lower, upper)


def random_literal_double():
    lower = np.finfo(np.float32).min
    upper = np.finfo(np.float32).max
    return np.random.uniform(lower, upper)


def random_literal_string():
    strlen = random.randint(0, 128)
    lower_letters = "abcdefghijklmnopqrstuvwxyz"
    upper_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    digits = "0123456789"
    cands = lower_letters + upper_letters + digits
    return "".join(random.choice(cands) for _ in range(strlen))

if __name__  == '__main__':
    d = random_literal_date()
    print(d)
    print(type(d))