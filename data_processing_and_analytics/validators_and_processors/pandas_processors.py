import os
import json

import pandas as pd


def df_from_file(path: str) -> pd.DataFrame:
    file_extension = os.path.splitext(path)[1]
    if file_extension == ".jsonl":
        return pd.DataFrame(_unpack_nested_lines(path))
    elif file_extension == ".csv":
        return pd.read_csv(path)


def _unpack_nested_lines(path: str) -> list:
    temp = []
    with open(path) as f:
        for line in f.readlines():
            row = json.loads(line)
            nested_row = row.get("events")
            if nested_row:
                temp.extend(nested_row)
            else:
                temp.append(row)
    return temp
