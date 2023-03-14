from inspect import signature
import math


def if_not_null(value):
    if value is None:
        return False
    elif value == "None":
        return False
    elif value == "nan":
        return False
    elif value == "":
        return False
    else:
        return True


def if_pandas_params_exist(mode, passed_params, real_params):
    passed_params = set(passed_params)
    real_params = set(real_params)

    diff = passed_params.difference(real_params)

    if len(diff):
        raise Exception(f"passed non-existing params for {mode} methods: {diff}")


def get_source_pandas_arguments(method):
    return signature(method).parameters.keys()
