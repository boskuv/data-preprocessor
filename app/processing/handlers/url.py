from urlextract import URLExtract

from app.utils.common import if_not_null


extractor = URLExtract()


def extract_url_from_line(text: str) -> str:
    try:
        if len(extractor.find_urls(text)):
            return str(extractor.find_urls(text)[0])
    except Exception as ex:
        print(f"ERROR | extract_url_from_line(..): {ex}")


def delete_url_from_line(text: str) -> str:
    if if_not_null(str(text)):
        try:
            extracted_url = extract_url_from_line(text)
            if extracted_url is not None:
                return text.replace(extracted_url, "")
        except Exception as ex:
            print(f"ERROR | delete_url_from_line(..): {ex}")
