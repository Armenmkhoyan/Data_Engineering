import os
from datetime import datetime
from typing import List, Union

from dateutil.parser import ParserError, parse

from schema_and_structures.tables_structure import FILE_EXTENSIONS


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


def is_extension_match(file_ext: str, ext: str) -> bool:
    return file_ext.lower() == ext.lower()


def get_corresponding_table_name(file_name: str, table_names: List[str]) -> Union[str, None]:
    if not file_name or len(table_names) < 1:
        return
    root, ext = os.path.splitext(file_name)
    root = root.strip("0123456789-_ ")
    for table in table_names:
        if root in table:
            return table


def get_file_extension(file_name: str) -> Union[str, None]:
    file_extension = file_name.split(".")[1]
    return FILE_EXTENSIONS.get(file_extension, "")
