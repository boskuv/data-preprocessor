import pandas as pd
from app.utils.common import get_source_pandas_arguments, if_pandas_params_exist


def __read_from_csv(path, pandas_params):
    """read from file *.csv"""
    if_pandas_params_exist(
        "reading", pandas_params.keys(), get_source_pandas_arguments(pd.read_csv)
    )
    return pd.read_csv(path, **pandas_params)


def __read_from_json(path, pandas_params):
    """read from file *.json"""
    if_pandas_params_exist(
        "reading", pandas_params.keys(), get_source_pandas_arguments(pd.read_json)
    )
    return pd.read_json(path, **pandas_params)


def __read_from_jsoneachrow(path, pandas_params):
    """read from file *.json in jsoneachrow format"""
    if_pandas_params_exist(
        "reading", pandas_params.keys(), get_source_pandas_arguments(pd.read_json)
    )
    if "lines" in pandas_params:
        pandas_params.pop("lines")
    return pd.read_json(path, lines=True, **pandas_params)


def __read_from_excel(path, pandas_params):
    """read from file *.xlsx"""
    if_pandas_params_exist(
        "reading", pandas_params.keys(), get_source_pandas_arguments(pd.read_excel)
    )
    return pd.read_excel(path, **pandas_params)
