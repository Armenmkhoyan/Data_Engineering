import re
from urllib.parse import urlparse


def is_valid_text(text: str) -> bool:
    return isinstance(text, str) and len(text) > 2 and text.replace(" ", "").isalpha()


def is_valid_email(text: str) -> bool:
    patt = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    return isinstance(text, str) and bool(re.match(patt, text))


def is_valid_coordinate(location: str) -> bool:
    return bool(re.fullmatch(r"\d{2}\.\d{5}", location))


def is_digit(text: str) -> bool:
    return text.isdigit() if isinstance(text, str) else False


def is_valid_address(text: str) -> bool:
    return bool(re.fullmatch(r"^(\w+)\s?\w*", text))


def is_validate_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


def is_element_of_list_in_text(texts: list[str], text: str) -> bool:
    return any([elem.lower() in text.lower() for elem in texts])
