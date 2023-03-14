## app.processing.handlers.date
- `convert_date_from_utc(date: str | datetime.datetime) -> str` Перевод даты из формата UTC в гггг-мм-дд
- `parse_with_dateutil(date: str | datetime.datetime) -> str` Перевод даты из любого формата в гггг-мм-дд при помощи dateutil

## app.processing.handlers.email
- `extract_email(email: str) -> str` Извлечение email-адреса по регулярному выражению 

## app.processing.handlers.text
- `print_value(text: str)` Напечатать в консоль значение из ячейки
- `add_to_list(*args) -> list` Добавить значения из ячеек в список [.., .., ..]
- `concat_fields(*args) -> str` Склеивание строк
- `leave_only_digits_in_line(text: str) -> str` Оставить только цифры в строке
- `to_string(text: str) -> str` Приведение к строковому типу
- `to_lower(text: str) -> str` Приведение к нижнему регистру
- `to_upper(text: str) -> str` Приведение к верхнему регистру
- `remove_multiple_spaces(text: str) -> str` Заменить множественные пробелы одинарными
- `rstrip_digits(text: str) -> str` Удалить все цифры с правой части строки
- `lstrip_digits(text: str) -> str` Удалить все цифры с правой части строки
- `strip_digits(text: str) -> str` Удалить все цифры с обеих сторон строки
- `strip_trash_symbols(text: str) -> str` Удалить все служебные символы с обеих сторон строки
- `remove_html_special_symbols(text: str) -> str` Удалить специальные html символы

## app.processing.handlers.url
- `extract_url_from_line(text: str) -> str` Извлечение url из текста
- `delete_url_from_line(text: str) -> str` Удаление url из текста
