import argparse
import os
from argparse import Namespace

import psutil
from pyarrow import parquet as pq


def get_args() -> Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file", type=str, help="Path to file.Example: '/home/user/input.csv'"
    )
    parser.add_argument(
        "--output-folder",
        help="Path to output folder.Example: '/home/user/output'",
        type=str,
    )
    parser.add_argument(
        "--chunk-size", type=int, help="Number of lines per file chunk.Example: 1000 "
    )

    parser.add_argument(
        "--header", type=bool, help="If csv file has header or not.Example: True "
    )

    parser.add_argument(
        "--auto",
        type=bool,
        help="For automatic determination of chunk size.Example: True ",
    )

    args = parser.parse_args()
    return args


PARTITION_FACTOR = 10
MAX_PARTITION_SIZE_MB = 120


def get_num_rows(file_path: str, file_type: str) -> int:
    if file_type == "csv":
        row_count = 1
        with open(file_path, "r") as file:
            # Use enumerate to count lines, starting at 1 to count the header
            for _ in file:
                row_count += 1
        return row_count
    elif file_type == "parquet":
        parquet_file = pq.ParquetFile(file_path)
        row_count = parquet_file.metadata.num_rows
        return row_count


def get_memory_stats() -> tuple[int, int]:
    """Get total_ram, available_ram in bytes"""
    # Get the system's memory information
    memory_info = psutil.virtual_memory()
    # Total RAM in bytes
    total_ram = memory_info.total
    # Available RAM in bytes
    available_ram = memory_info.available
    return total_ram, available_ram


def get_file_size_on_disk(file_path: str):
    return os.path.getsize(file_path)  # size in bytes


def get_rows_per_chunk(file_path: str, file_type: str) -> int:
    print(f"Calculating number of rows in file {file_path} ")
    num_rows = get_num_rows(file_path, file_type)
    print(f"Num Rows : {num_rows}")
    file_zize_bytes = get_file_size_on_disk(file_path)
    print(f"File Size : {file_zize_bytes} B, {file_zize_bytes/1024**3} GB")
    _, available_ram_bytes = get_memory_stats()
    print(
        f"Available Memory : {available_ram_bytes} B, {available_ram_bytes/1024**3} GB"
    )
    one_tenth_bytes = int(available_ram_bytes / PARTITION_FACTOR)
    print(
        f"1/{PARTITION_FACTOR} Available Memory : {one_tenth_bytes} B, {one_tenth_bytes / 1024 ** 3} GB"
    )

    one_tenth_bytes = min(one_tenth_bytes, MAX_PARTITION_SIZE_MB * 1024**2)

    if file_zize_bytes < one_tenth_bytes:
        return num_rows
    else:
        chunks = int(file_zize_bytes / one_tenth_bytes) + 1
        print(f"Num chunks : {chunks}")
        if chunks > num_rows:
            return num_rows
        chunk_size = int(num_rows / chunks) + 1
        return chunk_size
