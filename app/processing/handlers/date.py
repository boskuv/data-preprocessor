import datetime
from dateutil import parser

from app.utils.common import if_not_null


def convert_date_from_utc(date: str | datetime.datetime) -> str:
    """
    Перевод даты из формата UTC в гггг-мм-дд.
    """
    if if_not_null(str(date)):
        try:
            d2 = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
            return d2.strftime("%Y-%m-%d")
        except Exception as ex:
            print(f"ERROR | convert_date_from_utc(..): {ex}")
        return ""


def parse_with_dateutil(date: str | datetime.datetime) -> str:
    """
    Перевод даты из любого формата в гггг-мм-дд при помощи dateutil.
    """
    if if_not_null(str(date)):
        try:
            if len(str(date)) < 6:
                return ""
            parsed_date = parser.parse(date).strftime("%Y-%m-%d")
            return str(parsed_date)
        except Exception as ex:
            print(f"ERROR | parse_with_dateutil(..): {ex}")
    return ""
