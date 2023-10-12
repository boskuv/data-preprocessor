import os
import shutil
from filesplit.split import Split
from filesplit.merge import Merge
from pyutil import filereplace
from pathlib import Path

from .processing.processor import Processor
from .reading.reader import Reader
from .writing.writer import Writer


class Pipeline:
    class TransferObject:
        intermediate_status = "OK"
        message = ""
        df = None

        def __init__(self, is_chunked):
            self.is_chunked = is_chunked

    def __init__(self, config, options, logger):
        self.chunking_config = config["chunking"]
        self.reading_config = config["reading"]
        self.processing_config = config["processing"]
        self.writing_config = config["writing"]

        self.options = options

        self.logger = logger

        self.tmp_input_dir = ""
        self.is_chunked = False
        self.to_save_column_names_while_splitting = config["reading"]["save_header"]
        self.column_names = list()

        self.broken_files = set()

    def __get_input_files(self, path_to_files):
        if not os.path.exists(path_to_files):
            raise FileNotFoundError("Wrong input file or file path")
        if os.path.isdir(path_to_files):
            input_files = list()
            for root, _, files in os.walk(path_to_files):
                for file in files:
                    path_to_file = os.path.join(root, file)
                    input_files.append(path_to_file)
            return input_files
        else:
            return [path_to_files]

    def __move_processed_file(self, path_to_file, path_to_move):
        try:
            source_filename = Path(path_to_file).name
        except:
            pass

        if not os.path.exists(path_to_move):
            os.makedirs(path_to_move)

        if os.path.exists(path_to_file):
            shutil.move(path_to_file, os.path.join(path_to_move, source_filename))

    def __change_manifest_before_merging(self):
        if self.is_chunked:
            manifest = os.path.join(self.tmp_input_dir, "manifest")

            with open(manifest, "r") as file:
                lines = file.readlines()

            with open(manifest, "w") as file:
                for line in lines:
                    filename = line.split(",")[0]
                    if filename in self.broken_files:
                        self.broken_files.remove(filename)
                    else:
                        file.write(line)

            tmp_subdir_from_input_dir = os.path.basename(
                os.path.normpath(self.tmp_input_dir)
            )

            shutil.move(
                manifest,
                os.path.join(
                    self.writing_config["path"],
                    tmp_subdir_from_input_dir,
                    Path(manifest).stem,
                ),
            )

    def __split_file_into_chunks(self, path_to_file):
        self.is_chunked = False

        file_size = os.stat(path_to_file).st_size / 1024 / 1024  # size in megabytes
        paths_to_chunks = list()

        if file_size > self.chunking_config["threshold_to_chunk_MB"]:
            input_dir = os.path.dirname(self.reading_config["path"])
            input_filename = Path(path_to_file).stem
            self.tmp_input_dir = os.path.join(input_dir, f"tmp_{input_filename}")

            user_answer_for_splitting = 'N'

            if not os.path.exists(self.tmp_input_dir):
                os.makedirs(self.tmp_input_dir)
            else:
                is_user_answer_not_valid = True
                while is_user_answer_not_valid:
                    user_answer_for_splitting = input(
                        f"Found '{self.tmp_input_dir}'. Probably processing was interrupted. Do you want to continue working with previous processing? Type 'Y' to continue or 'N' to create new pipeline: "
                    )
                    if user_answer_for_splitting.strip().lower() in ["y", "n"]:
                        is_user_answer_not_valid = False
                    else:
                        print("Possible options: Y/N. Try again...")

            if user_answer_for_splitting.strip().lower() == 'n':
                Split(path_to_file, self.tmp_input_dir).bylinecount(
                    self.chunking_config["lines_per_chunked_file"]
                )
 
            paths_to_chunks.extend(
                [
                    os.path.join(self.tmp_input_dir, path_to_chunk)
                    for path_to_chunk in os.listdir(self.tmp_input_dir)
                    if not path_to_chunk.__contains__("manifest")
                ]
            )

            if user_answer_for_splitting.strip().lower() == 'y':
                path_to_processed_chunks = os.path.join(
                    self.writing_config["path"], f"tmp_{input_filename}"
                )

                if not os.path.exists(path_to_processed_chunks):
                    self.logger.warning(f"Can't find {path_to_processed_chunks} with splitted files. Starting processing from the beginning...")
                else:
                    stemmed_processed_chunks = set(
                        [
                        path_to_processed_chunk.split(".")[0]
                        for path_to_processed_chunk in os.listdir(path_to_processed_chunks)
                        if not path_to_processed_chunk.__contains__("manifest")
                        ]
                    )

                    stemmed_input_chunks = set(
                        [
                            path_to_input_chunk.split(".")[0]
                            for path_to_input_chunk in os.listdir(self.tmp_input_dir)
                            if not path_to_input_chunk.__contains__("manifest")
                        ]
                    )
                    
                    chunks_to_process = list(stemmed_input_chunks.difference(stemmed_processed_chunks))

                    chunk_With_header = sorted(paths_to_chunks)[0]
                    paths_to_chunks.clear()

                    paths_to_chunks.extend(
                        [
                            os.path.join(self.tmp_input_dir, path_to_chunk)
                            for path_to_chunk in os.listdir(self.tmp_input_dir)
                            if path_to_chunk.split(".")[0] in chunks_to_process
                        ]
                    )
                    paths_to_chunks.append(chunk_With_header)
                    
                    broken_chunks = list()
                    broken_chunks.extend(
                        [
                            broken_chunk
                            for broken_chunk in os.listdir(self.writing_config["path_to_errors"])
                            if broken_chunk.__contains__("_") and broken_chunk.split(".")[0].split("_")[:-1] == chunk_With_header.split("\\")[-1].split(".")[0].split("_")[:-1]
                        ]
                    )

                    self.broken_files.update(broken_chunks)
                    

            self.is_chunked = True
            self.to_save_column_names_while_splitting = True
        else:
            paths_to_chunks.append(path_to_file)

        return sorted(paths_to_chunks)

    def __merge_file_from_chunks(self, path_to_file):
        if self.is_chunked:
            filename = Path(path_to_file).stem
            extension = Path(path_to_file).suffix

            tmp_output_dir = os.path.join(
                self.writing_config["path"], f"tmp_{filename}"
            )

            filereplace(
                os.path.join(tmp_output_dir, "manifest"),
                extension.lstrip("."),
                self.writing_config["type"],
            )

            Merge(
                tmp_output_dir,
                self.writing_config["path"],
                f"{filename}.{self.writing_config['type']}",
            ).merge(True)

            # clear tmp directories
            try:
                shutil.rmtree(self.tmp_input_dir)
                shutil.rmtree(tmp_output_dir)
            except OSError as e:
                raise Exception(e)

    def __validate_config(self, path_to_file):
        try:
            transfer_object = self.TransferObject(is_chunked=False)

            transfer_object = Reader(
                transfer_object,
                path_to_file,
                self.reading_config,
                self.column_names,
                True,
            ).read()

            if transfer_object.intermediate_status == "ERROR":
                raise ValueError(transfer_object.message)

            # clean column names in df
            cleaned_columns = [
                column.strip(" '") for column in transfer_object.df.columns
            ]
            transfer_object.df.rename(
                columns=dict(zip(transfer_object.df.columns, cleaned_columns)),
                inplace=True,
            )

            transfer_object = Processor(
                transfer_object, self.processing_config, Path(path_to_file).name
            ).process()

            if transfer_object.intermediate_status == "ERROR":
                raise ValueError(transfer_object.message)

            if self.writing_config["type"].lower() == "jsoneachrow":
                valuable_fields = set(self.writing_config["valuable_fields"].fields)
                passed_fields = set(transfer_object.df.columns)

                # case when 'valuable_fields_config.fields' is empty
                if len(valuable_fields) == 0:
                    valuable_fields = passed_fields

                diff = valuable_fields.difference(passed_fields)

                if len(diff) > 0:
                    transfer_object.intermediate_status = "ERROR"
                    error_message = f"Got non-existing fields from config (writing.valuable_fields.fields): {diff}. Please use mapped values from previous step!"
                    transfer_object.message = error_message
                    raise Exception(f"{error_message}")

        except Exception as ex:
            transfer_object.intermediate_status = "ERROR"
            self.logger.error(f"{ex}")

        if transfer_object.intermediate_status == "ERROR":
            return False
        else:
            return True

    def __pipe(
        self,
        path_to_chunked_file,
        path_to_file,
        is_chunked,
        to_save_column_names_while_splitting,
    ):
        transfer_object = self.TransferObject(is_chunked)

        transfer_object = Reader(
            transfer_object,
            path_to_chunked_file,
            self.reading_config,
            self.column_names,
            False,
        ).read()

        if transfer_object.intermediate_status == "ERROR":
            raise ValueError(transfer_object.message)

        # clean column names in df
        cleaned_columns = [column.strip(" '") for column in transfer_object.df.columns]
        transfer_object.df.rename(
            columns=dict(zip(transfer_object.df.columns, cleaned_columns)),
            inplace=True,
        )

        if to_save_column_names_while_splitting:
            self.column_names = transfer_object.df.columns

        transfer_object = Processor(
            transfer_object, self.processing_config, Path(path_to_file).name
        ).process()
        transfer_object = Writer(
            transfer_object, path_to_chunked_file, self.writing_config
        ).write()

    def start(self):
        pipeline_status = "OK"

        for path_to_file in self.__get_input_files(self.reading_config["path"]):
            self.logger.info(f"{path_to_file} : VALIDATING...")
            if self.__validate_config(path_to_file):
                self.logger.info(f"{path_to_file} : STARTING...")
                chunks_from_splitted_file = self.__split_file_into_chunks(path_to_file)
                processed_chunks_counter = 0

                for path_to_chunked_file in chunks_from_splitted_file:
                    try:
                        self.__pipe(
                            path_to_chunked_file,
                            path_to_file,
                            self.is_chunked,
                            self.to_save_column_names_while_splitting,
                        )

                    except Exception as error_message:
                        pipeline_status = "NOT OK OR PARTIALLY OK"

                        self.logger.error(f"{path_to_chunked_file} : {error_message}")

                        self.broken_files.add(os.path.basename(path_to_chunked_file))

                        self.__move_processed_file(
                            path_to_chunked_file, self.writing_config["path_to_errors"]
                        )

                    # because we need to save column_names only from first splitted file
                    self.to_save_column_names_while_splitting = False

                    processed_chunks_counter += 1
                    self.logger.info(
                        f"{processed_chunks_counter}/{len(chunks_from_splitted_file)} {path_to_chunked_file} : PROCESSED"
                    )

                self.__change_manifest_before_merging()
                self.__merge_file_from_chunks(path_to_file)

                self.logger.info(f"{path_to_file} : {pipeline_status}")

                self.__move_processed_file(
                    path_to_file, self.writing_config["path_to_processed"]
                )
            else:
                self.logger.error(
                    f"{path_to_file}: IDENTIFIED AN ERROR WHILE VALIDATION"
                )

        print("FINISHED")
