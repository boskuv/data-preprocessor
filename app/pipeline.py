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
        self.to_save_column_names = False
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
            file = Path(path_to_file).name
        except:
            pass

        if not os.path.exists(path_to_move):
            os.makedirs(path_to_move)

        if os.path.exists(path_to_file):
            shutil.move(path_to_file, os.path.join(path_to_move, file))

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

            shutil.move(
                manifest,
                os.path.join(self.writing_config["path"], "tmp", Path(manifest).stem),
            )

    def __split_file_into_chunks(self, path_to_file):
        self.is_chunked = False

        file_size = os.stat(path_to_file).st_size / 1024 / 1024  # size in megabytes
        path_to_chunks = list()
        if self.options.only_read:
            path_to_chunks.append(path_to_file)
        elif file_size > self.chunking_config["threshold_to_chunk_MB"]:
            input_dir = os.path.dirname(self.reading_config["path"])
            self.tmp_input_dir = os.path.join(input_dir, "tmp")

            if not os.path.exists(self.tmp_input_dir):
                os.makedirs(self.tmp_input_dir)

            Split(path_to_file, self.tmp_input_dir).bylinecount(
                self.chunking_config["lines_per_chunked_file"]
            )

            path_to_chunks.extend(
                [
                    os.path.join(self.tmp_input_dir, path_to_chunk)
                    for path_to_chunk in os.listdir(self.tmp_input_dir)
                    if not path_to_chunk.__contains__("manifest")
                ]
            )

            self.is_chunked = True
            self.to_save_column_names = True
        else:
            path_to_chunks.append(path_to_file)

        return sorted(path_to_chunks)

    def __merge_file_from_chunks(self, path_to_file):
        if self.is_chunked:

            tmp_output_dir = os.path.join(self.writing_config["path"], "tmp")

            filename = Path(path_to_file).stem
            extension = Path(path_to_file).suffix

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

            # clear input
            try:
                shutil.rmtree(self.tmp_input_dir)
                shutil.rmtree(tmp_output_dir)
            except OSError as e:
                raise Exception(e)

    def __pipe(self, path_to_file, is_chunked, to_save_column_names):
        transfer_object = self.TransferObject(is_chunked)

        transfer_object = Reader(
            transfer_object,
            path_to_file,
            self.reading_config,
            self.column_names,
            self.options.only_read,
        ).read()

        if not self.options.only_read:

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

            if to_save_column_names:
                self.column_names = transfer_object.df.columns

            transfer_object = Processor(
                transfer_object, self.processing_config
            ).process()
            transfer_object = Writer(
                transfer_object, path_to_file, self.writing_config
            ).write()

    def start(self):

        pipeline_status = "OK"

        for path_to_file in self.__get_input_files(self.reading_config["path"]):

            chunks_from_splitted_file = self.__split_file_into_chunks(path_to_file)
            processed_chunks_counter = 0

            for path_to_chunked_file in chunks_from_splitted_file:

                processed_chunks_counter += 1
                
                try:
                    self.__pipe(
                        path_to_chunked_file, self.is_chunked, self.to_save_column_names
                    )
                    self.logger.info(
                        f"{processed_chunks_counter}/{len(chunks_from_splitted_file)} {path_to_chunked_file} : PROCESSED"
                    )

                except Exception as error_message:
                    pipeline_status = "NOT OK OR PARTIALLY OK"

                    self.logger.error(f"{path_to_chunked_file} : {error_message}")

                    self.broken_files.add(os.path.basename(path_to_chunked_file))

                    self.__move_processed_file(
                        path_to_chunked_file, self.writing_config["path_to_errors"]
                    )

                    self.logger.error(
                        f"{processed_chunks_counter}/{len(chunks_from_splitted_file)} {path_to_chunked_file} : MOVED TO ERRORS"
                    )

                self.to_save_column_names = False

            if not self.options.only_read:
                self.__change_manifest_before_merging()
                self.__merge_file_from_chunks(path_to_file)

                self.logger.info(f"{path_to_file} : {pipeline_status}")

                self.__move_processed_file(
                    path_to_file, self.writing_config["path_to_processed"]
                )

        print("FINISHED")
