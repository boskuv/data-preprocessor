from re import IGNORECASE, compile
import numpy as np

from app.utils.common import if_not_null


email_pattern = compile(r"\w+@[a-zA-Z_]+?\.[a-zA-Z]{2,6}", IGNORECASE)


def extract_email(email: str) -> str:
    if if_not_null(str(email)):
        email = str(email).strip()
        match = email_pattern.findall(email)
        return match[0] if len(match) and len(email.split("@")[0]) > 2 else np.NaN
