from inspect import signature
import json

from app.utils.common import (
    if_not_null,
    get_source_pandas_arguments,
    if_pandas_params_exist,
)


def __write_to_csv(path, pandas_params, valuable_fields, df):
    """write to file in csv format"""
    if_pandas_params_exist(
        "writing", pandas_params.keys(), get_source_pandas_arguments(df.to_csv)
    )
    with open(path, "w", encoding="utf-8") as file:
        df.to_csv(path, **pandas_params)


def __write_to_json(path, pandas_params, valuable_fields, df):
    """write to file in json format"""
    if_pandas_params_exist(
        "writing", pandas_params.keys(), get_source_pandas_arguments(df.to_json)
    )
    with open(path, "w", encoding="utf-8") as file:
        df.to_json(path, **pandas_params)


def __write_to_jsoneachrow(path, pandas_params, valuable_fields_config, df):
    """write to file in jsoneachrow format"""
    columns = df.columns
    valuable_fields = set(valuable_fields_config.fields)

    with open(path, "w", encoding="utf-8") as file:
        for row in df.to_numpy().tolist():
            json_row = dict(zip(columns, row))
            cleaned_json_row = {
                k: v for k, v in json_row.items() if if_not_null(str(v))
            }

            passed_fields = set(cleaned_json_row.keys())

            # case when 'valuable_fields_config.fields' is empty
            if len(valuable_fields) == 0:
                valuable_fields = passed_fields

            in_common = valuable_fields.intersection(passed_fields)

            if valuable_fields_config.rule == "strict":
                if valuable_fields == in_common:
                    file.write(
                        str(f"{json.dumps(cleaned_json_row, ensure_ascii=False)}\n")
                    )
            else:
                valuable_fields_config.loyal_boundary_value = (
                    len(valuable_fields)
                    if valuable_fields_config.loyal_boundary_value
                    > len(valuable_fields)
                    else valuable_fields_config.loyal_boundary_value
                )
                if len(in_common) >= valuable_fields_config.loyal_boundary_value:
                    file.write(
                        str(f"{json.dumps(cleaned_json_row, ensure_ascii=False)}\n")
                    )
