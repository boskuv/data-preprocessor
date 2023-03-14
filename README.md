# Data Preprocessor
Project for building flexible ETL pipelines
____
## Requirements
Python 3.10
[docker]
____
## Quick Start
- create directories: 
  - /input
  - /output
  - /logs
  - /processed/broken
- cp config.yaml.example config.yaml
- build a docker image, using `docker build -t <image_name> .` 
- edit 'config.yaml', according to your target
- add files to '/input'
- run a docker container, using compose `docker-compose up` 

____
## Configuration setting
### Chunking `(required)`:
- threshold_to_chunk_MB: the threshold size of file (MB) to be chunked `500 MB is a default value`
- lines_per_chunked_file: how many lines should be in a chunked file (equal or less) `100000 lines is a default value`
### Reading `(required)`:
> Be sure that you looked at the content of the file carefully in order to define the input type properly

> Remember! YAML is space-sensetive. For YAML tricks look [here](https://helm.sh/docs/chart_template_guide/yaml_techniques/)


- type `(required)`: type of input data (valid types: csv, json, jsoneachrow, excel) 
- path `(required)`: path to input directory or file 
- pandas_params: arguments for pandas reading methods. For more details: [read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html) / [read_json](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html) / [read_excel](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html)

#### Example
```
reading:
    type: csv
    path: input/ 
    pandas_params:
        encoding: utf-8
        delimiter: "|"
        on_bad_lines: skip
```

### Processing:
- mapping: the list of key-value pairs to rename existing columns (**NOTE**: It's case-sensetive)
- fields_with_static_values: the list of key-value pairs represents the column and with what const value it will be filled 
- handlers: the list of methods and its arguments for applying to existing columns (**NOTE**: 1. Order of methods is important; 2. The result will be written into column, which is pointed first)
- fields_to_drop: the list of columns to delete
- fields_to_append: the list of columns to add (**NOTE**: These columns will be filled with empty values)
> Order of processing: mapping -> appending -> adding fields with static values -> handling -> droping

#### Example
```
processing:
    mapping:
        database: db
        date_of_leak: date_leak
    fields_to_append: [phone]    
    fields_with_static_values:
        source: test_source
        date_of_leak: 2022-09-10
    handlers:
        concat_fields: 
            - [name, other_info]
            - [name, number]
        lowerize:
            - [name]
    fields_to_drop: [other_info]
```

## Writing `(required)`:
- type `(required)`: type of output data (valid types: csv, json, jsoneachrow)
- path `(required)`: path to output directory (**NOTE**: The name of file will be saved the same as input)
- path_to_processed `(required)`: path for moving successfully processed input files
- path_to_errors `(required)`: path for moving broken input files
- valuable_fields (for jsoneachrow): fields that must have not null values in a row to be written in a file. There're 2 rules: strict (all fields from list must have not null value) and loyal (at least 'loyal_boundary_value' amount of fields from list must have not null value).
- pandas_params (for csv, json types): arguments for pandas writing methods. For more details: [to_csv](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html) / [to_json](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html#)

#### Example
```
writing:
    type: jsoneachrow
    path: output/
    path_to_processed: processed/
    path_to_errors: processed/broken
    valuable_fields:
        fields: [phone, name]
        rule: loyal
        loyal_boundary_value: 1 
    pandas_params: 
        encoding: utf-8 
```
____

## Контрибьютинг

Новые данные в главной ветке

появляются только после успешного прохождения код-ревью , посредством мерджа.

Перед каждым пушем `isort .` , а потом `black .`

**ТРЕБОВАНИЯ ПРИ ДОБАВЛЕНИИ НОВЫХ ОБРАБОТЧИКОВ**  

При добавлении новых групп обработчиков:
1. В директории app/processing/handlers добавить группу <new_group_name>.py
2. В файле `app/processing/handlers/__init__.py` добавить строку from .<new_group_name> import *
3. Наполнить <new_group_name>.py необходимыми методами

Желательно:
- проверять вход на пустые значения для избежания лишних ошибок
```
from app.utils.common import if_not_null

def convert_date_from_utc(date: str | datetime):
    """
    Перевод даты из формата UTC в гггг-мм-дд.
    """   
    if if_not_null(str(date)):
   ...
```

Обязательно:
- добавлять docstring в формате:
```
    """
    Пример docstring.
    """   
```
- в критичных местах использовать try/except

Для отслеживания ошибок:
- raise Exception(...) - обработка всего файла прекращается, он падает в ошибки
- print(...) - вывод ошибки в консоль с продолжением обработки других строк
- pass - ошибка игнорируется