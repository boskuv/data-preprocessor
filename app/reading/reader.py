import os

from . import file


class Reader:
    def __init__(self, transfer_object, path, config, column_names, only_read):
        """constructor for Reader()"""
        self.transfer_object = transfer_object
        self.__path = path
        self.__type = config["type"].lower()
        self.__pandas_params = config["pandas_params"]
        self.__column_names = column_names
        self.__only_read = only_read
        self.__class_name = self.__class__.__name__

    def __get_reader(self):
        """resolve reader method by type from config"""
        if self.__type == "csv":
            return getattr(file, "__read_from_csv")
        elif self.__type == "json":
            return getattr(file, "__read_from_json")
        elif self.__type == "jsoneachrow":
            return getattr(file, "__read_from_jsoneachrow")
        elif self.__type == "excel":
            return getattr(file, "__read_from_excel")
        else:
            raise ValueError("Non-existing reader type")

    def read(self):
        """main method for Reader()"""
        if os.path.exists(self.__path):
            reader = self.__get_reader()

            if self.__only_read:
                self.__pandas_params["nrows"] = 100

                try:
                    self.transfer_object.df = reader(self.__path, self.__pandas_params)
                    print(f"{self.__path}: ")
                    print(self.transfer_object.df.columns)
                    print(self.transfer_object.df)
                    print()
                except Exception as ex:
                    print(f"ERROR | {self.__class_name} | {ex}")
            else:
                # add saved header to splitted csv files
                if self.__type == "csv" and len(self.__column_names):
                    self.__pandas_params.update(
                        {"names": self.__column_names, "header": None}
                    )
                try:
                    self.transfer_object.df = reader(self.__path, self.__pandas_params)
                    self.transfer_object.intermediate_status = "OK"
                except Exception as ex:
                    self.transfer_object.intermediate_status = "ERROR"
                    self.transfer_object.message = f"{self.__class_name} | {ex}"
        else:
            raise Exception(f"{self.__path}: file doesn't exist")

        return self.transfer_object
