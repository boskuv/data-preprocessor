from cgitb import handler
import inspect
import numpy as np
from pandas import DataFrame
from functools import wraps
import os
import contextlib
from alive_progress import alive_bar


from . import handlers


class Processor:
    def __init__(self, transfer_object, config, path_to_file):
        """Constructor Processor()

        Args:
            config (dict): configuration from yaml
        """
        if transfer_object.intermediate_status == "ERROR":
            raise ValueError(transfer_object.message)
        self.transfer_object = transfer_object
        self.__config = config
        # self.__path_to_file = path_to_file
        self.__class_name = self.__class__.__name__

    def check_none_df_decorator(process_df):
        """Decorator for checking if there is an empty Dataframe after processing

        Args:
            process_df: decorated method for processing Dataframe
        """

        @wraps(process_df)
        def check_none_df(*args):
            self = args[0]
            method_name = process_df.__name__

            process_df(*args)

            if self.transfer_object.df is None:
                error_message = f"Got empty Dataframe after applying method"
                raise Exception(f"{method_name} | {error_message}")

        return check_none_df

    def map_fields_decorator(process_df):
        """Decorator which gets fields from mapping, i.e. no matter what field of mapping you will use - key or value

        Args:
            process_df: decorated method for processing Dataframe
        """

        @wraps(process_df)
        def map_fields(*args):
            self = args[0]
            method_name = process_df.__name__

            current_fields = set(self.transfer_object.df.columns)

            passed_fields = set(args[1])

            diffs = passed_fields.difference(current_fields)

            if len(diffs):
                error_fields = set()

                for diff in diffs:
                    if diff in self.__config["mapping"]:
                        args[1].remove(diff)
                        args[1].append(self.__config["mapping"][diff])
                    else:
                        error_fields.add(diff)

                if len(error_fields):
                    print(
                        f"WARNING | {method_name} | Got non-existing fields {error_fields}. Please check your config!"
                    )

            process_df(*args)

        return map_fields

    def repeats_decorator(process_df):
        """Decorator for checking if there are any repeated fields in Dataframe

        Args:
            process_df: decorated method for processing Dataframe
        """

        @wraps(process_df)
        def check_fields_repeats(*args):
            self = args[0]
            method_name = process_df.__name__

            process_df(*args)
            fields = list(self.transfer_object.df.columns)
            repeated_fields = set()

            for field in fields:
                if fields.count(field) > 1:
                    repeated_fields.add(field)

            if len(repeated_fields):
                error_message = f"Got repeated fields in Dataframe: {repeated_fields}"
                raise Exception(f"{method_name} | {error_message}")

        return check_fields_repeats

    def block_print_decorator(process_df):
        """Decorator for blocking print() from handlers

        Args:
            process_df: decorated method for processing Dataframe
        """

        @wraps(process_df)
        def block_print(*args):
            if False:  # TODO: cmd flag
                with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
                    process_df(*args)
            else:
                process_df(*args)

        return block_print

    def __if_fields_exist(self, config_columns, config_node_with_fields):
        """Check if fields from config exist and match to input

        Args:
            config_columns (list): fields from config for processing
            config_node_with_fields (str): node from config where you point your fields

        Returns:
            if columns exist - True, otherwise - raise an Exception
        """
        method_name = self.__if_fields_exist.__name__
        df_columns = set(self.transfer_object.df.columns)

        if config_node_with_fields != "mapping":
            # to bind column's names before and after mapping
            df_columns.update(self.__config["mapping"].keys())

        mapping_columns = set(config_columns)
        diff = mapping_columns.difference(df_columns)
        if len(diff) == 0:
            return True
        else:
            error_message = f"Got non-existing fields from config ({config_node_with_fields}): {diff}"
            raise Exception(f"{method_name} | {error_message}")

    @check_none_df_decorator
    @repeats_decorator
    def __rename_fields(self):
        """Rename input fields according to mapping from config"""
        self.transfer_object.df.rename(columns=self.__config["mapping"], inplace=True)

    @check_none_df_decorator
    @repeats_decorator
    def __append_fields(self):
        """Add additional fields to Dataframe"""
        if self.__config["fields_to_append"] is not None:
            self.transfer_object.df = self.transfer_object.df.reindex(
                self.transfer_object.df.columns.tolist()
                + list(set(self.__config["fields_to_append"])),
                axis=1,
                fill_value="",
            )

    @check_none_df_decorator
    def __add_fields_with_static_values(self):
        """Add fields with static(const) values"""
        self.transfer_object.df = self.transfer_object.df.assign(
            **{
                column: str(self.__config["fields_with_static_values"][column])
                for column in list(set(self.__config["fields_with_static_values"]))
            }
        )

    @check_none_df_decorator
    @map_fields_decorator
    def __drop_fields(self, fields_to_drop):
        """Delete odd fields from Dataframe"""
        if fields_to_drop is not None:
            DataFrame.drop
            """Delete fields from input columns"""
            self.transfer_object.df = self.transfer_object.df.drop(
                list(set(fields_to_drop)), axis=1
            )

    def __if_method_exists(self, handler_name):
        """Check if method from config exists according to its name

        Args:
            method_name (str): the name of the method

        Returns:
            if method exists - True, otherwise - raise an Excaption
        """
        try:
            handler = getattr(handlers, handler_name)
            inspect.getfullargspec(handler)
            return True
        except Exception as e:
            error_message = f"Got non-existing method from config : {handler_name}. Check your handlers!"
            raise Exception(f"{error_message}")

    @check_none_df_decorator
    @repeats_decorator
    @map_fields_decorator
    @block_print_decorator
    def __apply(self, handler_args, handler_name):
        """Handle dataframe column with specific methods from 'handlers.py'

        Args:
            method_name (str): the name of the method
            method_args (list): _description_
        """
        handler = getattr(handlers, handler_name)
        try:
            self.transfer_object.df[handler_args[0]] = self.transfer_object.df[
                handler_args
            ].apply(lambda x: handler(*x), axis=1)
        except Exception as ex:
            raise Exception(f"{handler_name} | {ex}")

    def process(self):
        """Main method for Processor(), which includes all stages of processing

        Returns:
            pandas.DataFrame: processed Dataframe
        """
        if self.__config is not None:
            try:
                if self.__if_fields_exist(self.__config["mapping"].keys(), "mapping"):
                    self.__rename_fields()

                self.__append_fields()

                self.__add_fields_with_static_values()

                print("HANDLING STATUS:")
                with alive_bar(len(self.__config["handlers"])) as bar:
                    for method, fields_for_processing in self.__config[
                        "handlers"
                    ].items():
                        if self.__if_method_exists(method):
                            for args in fields_for_processing:
                                if self.__if_fields_exist(args, "handlers"):
                                    self.__apply(args, method)
                        bar()

                if self.__if_fields_exist(
                    self.__config["fields_to_drop"], "fields_to_drop"
                ):
                    self.__drop_fields(self.__config["fields_to_drop"])

                self.transfer_object.df = self.transfer_object.df.replace(
                    r"^\s*$", np.nan, regex=True
                )

                # delete columns filled with NaN values
                # self.transfer_object.df.dropna(axis=1, how="all", inplace=True)

                self.transfer_object.df.drop_duplicates(keep=False, inplace=True)

            except Exception as ex:
                self.transfer_object.intermediate_status = "ERROR"
                self.transfer_object.message = f"{self.__class_name} | {ex}"

        return self.transfer_object
