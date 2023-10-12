import re
import numpy as np

from app.utils.common import if_not_null


def print_value(text: str):
    if if_not_null(str(text)):
        print(text)
        print()


def add_to_list(*args) -> list:
    try:
        result = [str(arg) for arg in args if if_not_null(str(arg))]
        if len(result):
            return str(result)
        else:
            return np.NaN
    except Exception as ex:
        print(f"ERROR | add_to_list(..): {ex}")


def concat_fields(*args) -> str:
    result = " ".join(
        [
            str(arg).strip()
            for arg in args
            if str(arg).strip() != "" and if_not_null(str(arg))
        ]
    )
    return result.strip()


def leave_only_digits_in_line(text: str) -> str:
    if if_not_null(str(text)):
        if len(re.findall("\d+", text)):
            return "".join(re.findall("\d+", text)[0])


def to_string(text: str) -> str:
    try:
        return str(text)
    except Exception as ex:
        print(f"ERROR | to_string(..): {ex}")


def to_lower(text: str) -> str:
    if isinstance(text, str):
        try:
            return str(text).strip().lower()
        except Exception as ex:
            print(f"ERROR | to_lower(..): {ex}")


def to_upper(text: str) -> str:
    if isinstance(text, str):
        try:
            return str(text).strip().upper()
        except Exception as ex:
            print(f"ERROR | to_upper(..): {ex}")


def remove_multiple_spaces(text: str) -> str:
    if isinstance(text, str):
        try:
            return re.sub(" {2,}", " ", text)
        except Exception as ex:
            print(f"ERROR | remove_multiple_spaces(..): {ex}")


def rstrip_digits(text: str) -> str:
    if isinstance(text, str):
        try:
            return text.rstrip("0123456789 ")
        except Exception as ex:
            print(f"ERROR | rstrip_digits(..): {ex}")


def lstrip_digits(text: str) -> str:
    if if_not_null(str(text)):
        try:
            return text.lstrip("0123456789 ")
        except Exception as ex:
            print(f"ERROR | lstrip_digits(..): {ex}")


def strip_digits(text: str) -> str:
    if if_not_null(str(text)):
        try:
            return text.strip("0123456789 ")
        except Exception as ex:
            print(f"ERROR | strip_digits(..): {ex}")


def strip_trash_symbols(text: str) -> str:
    if if_not_null(str(text)):
        try:
            return text.strip("'.\,-=?!#:;^& ")
        except Exception as ex:
            print(f"ERROR | strip_trash_symbols(..): {ex}")


def remove_html_special_symbols(text: str) -> str:
    try:
        if if_not_null(text):
            reserved_chars = [
                "&quot;",
                "&amp;",
                "&lt",
                "&gt;",
                "&quot",
                "&amp",
                "&lt",
                "&gt",
            ]
            if text.__contains__("&#"):
                cleaned_text = "".join(
                    symbol for symbol in text if symbol.isalpha() or symbol.isspace()
                )
                return cleaned_text
            elif any(char in text for char in reserved_chars):
                for word in text.split():
                    word_sequence = list()
                    if word not in reserved_chars:
                        word_sequence.append(word)
                return " ".join(word_sequence)
            else:
                return text
    except Exception as ex:
        print(f"ERROR | remove_html_special_symbols(..): {ex}")
