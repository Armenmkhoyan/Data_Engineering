from datetime import datetime

from dateutil.parser import ParserError, parse


def del_elem_by_key(x: dict, key: str) -> dict:
    del x[key]
    return x


def convert_timestamp(x: dict, key: str) -> dict:
    x[key] = datetime.fromtimestamp(x[key]).strftime('%y-%m-%d %H:%M:%S')
    return x


def parse_date(date: str) -> datetime:
    try:
        return parse(date, fuzzy=True)
    except ParserError:
        raise ValueError(f"{date} not a valid data")


def set_element_to_none(x: dict, key: str) -> dict:
    x[key] = None
    return x
