import re


def parse_int(str):
    try:
        str = re.sub(r'\D', '', str)
        return int(str)
    except ValueError:
        return None


def normalize_str_array(arr):
    arr = [s.strip() for s in arr]
    arr = list(filter(lambda s: s != '', arr))

    return arr
