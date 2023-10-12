import os
from pathlib import Path

from . import file


class Writer:
    def __init__(self, transfer_object, path_to_file, config):
        """constructor for Writer()"""
        if transfer_object.intermediate_status == "ERROR":
            raise ValueError(transfer_object.message)
        self.transfer_object = transfer_object
        self.__path_to_file = path_to_file
        self.__type = config["type"].lower()
        self.__output_directory = config["path"]
        self.__pandas_params = config["pandas_params"]
        self.__valuable_fields = config["valuable_fields"]
        self.__class_name = self.__class__.__name__

        os.makedirs(os.path.dirname(config["path"]), exist_ok=True)

    def __prepare_output_directory(self):
        """create temporary directory for output chunked files"""
        try:
            filename = Path(self.__path_to_file).stem
        except:
            pass

        if self.transfer_object.is_chunked:
            source_filemame = filename[: filename.rfind("_")]
            self.__output_directory = os.path.join(
                self.__output_directory, f"tmp_{source_filemame}"
            )

        if not os.path.exists(self.__output_directory):
            os.makedirs(self.__output_directory)

        return os.path.join(self.__output_directory, f"{filename}.{self.__type}")

    def __get_writer(self):
        """resolve writer method by type from config"""
        if self.__type == "csv":
            return getattr(file, "__write_to_csv")
        elif self.__type == "json":
            return getattr(file, "__write_to_json")
        elif self.__type == "jsoneachrow":
            return getattr(file, "__write_to_jsoneachrow")
        else:
            raise ValueError("Non-existing writer type")

    def write(self):
        """main method for Writer()"""
        try:
            output_path = self.__prepare_output_directory()
            writer = self.__get_writer()
            self.transfer_object.df = writer(
                output_path,
                self.__pandas_params,
                self.__valuable_fields,
                self.transfer_object.df,
            )
            self.transfer_object.intermediate_status = "OK"
        except Exception as ex:
            self.transfer_object.intermediate_status = "ERROR"
            self.transfer_object.message = f"{self.__class_name} | {ex}"
            raise Exception(self.transfer_object.message)

        return self.transfer_object
