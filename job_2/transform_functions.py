import re
from datetime import datetime


def del_elem_by_key(x: dict, key: str) -> dict:
    del x[key]
    return x


def convert_timestamp(x: dict, key: str) -> dict:
    x[key] = datetime.fromtimestamp(x[key]).strftime('%y-%m-%d %H:%M:%S')
    return x


def set_element_to_none(x: dict, key: str) -> dict:
    x[key] = None
    return x


def is_valid_text(text: str) -> bool:
    return isinstance(text, str) and len(text) > 2 and text.replace(" ", "").isalpha()


def is_valid_email(text: str) -> bool:
    patt = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    return isinstance(text, str) and bool(re.match(patt, text))
