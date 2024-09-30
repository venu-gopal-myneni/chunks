import csv
import os
from pathlib import Path

from littlechunks.utils import get_rows_per_chunk


def list_of_lists_to_csv(data: list[list], file_path: str, newline: str, encoding: str):
    with open(file_path, mode="w", newline=newline, encoding=encoding) as file:
        writer = csv.writer(file)
        writer.writerows(data)


def csv_row_generator(
    file_path: str,
    chunk_size: int,
    header: bool,
    delimiter: str,
    newline: str,
    encoding: str,
):
    with open(file_path, mode="r", newline=newline, encoding=encoding) as file:
        reader = csv.reader(file, delimiter=delimiter)
        chunk = []
        first_chunk = True
        for i, row in enumerate(reader):
            if i == 0:
                header_row = row
            chunk.append(row)
            if (i + 1) % chunk_size == 0:
                if header and not first_chunk:
                    chunk = [header_row] + chunk
                yield chunk
                first_chunk = False
                chunk = []
        if chunk:  # Yield any remaining rows that didn't make up a full chunk
            if header and not first_chunk:
                chunk = [header_row] + chunk
            yield chunk


def split_csv_file_auto(
    file_path: str,
    output_folder: str,
    header: bool,
    delimiter: str = ",",
    newline: str = "",
    encoding: str = "utf-8",
):
    chunk_num_rows = get_rows_per_chunk(file_path, "csv")
    print(f"Chunk Num Rows : {chunk_num_rows}")
    split_csv_file(
        file_path,
        output_folder,
        chunk_num_rows,
        header,
        delimiter,
        newline,
        encoding,
    )


def split_csv_file(
    file_path: str,
    output_folder: str,
    chunk_num_rows: int,
    header: bool,
    delimiter: str = ",",
    newline: str = "",
    encoding: str = "utf-8",
):
    os.makedirs(output_folder, exist_ok=True)

    for pos, chunk in enumerate(
        csv_row_generator(
            file_path, chunk_num_rows, header, delimiter, newline, encoding
        )
    ):
        output_file_path = str(Path(output_folder, f"chunk_{pos}.csv"))
        list_of_lists_to_csv(chunk, output_file_path, newline, encoding)
        print(
            f"Processed chunk number {pos} with {len(chunk)} rows, file saved at {output_file_path}"
        )
