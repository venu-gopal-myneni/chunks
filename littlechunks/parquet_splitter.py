import pyarrow.parquet as pq
import pyarrow as pa
import os
from littlechunks.utils import get_rows_per_chunk


def split_parquet_file(file_path: str, output_folder: str, chunk_num_rows: int):
    parquet_file = pq.ParquetFile(file_path)

    os.makedirs(output_folder, exist_ok=True)

    # Iterate over the file in batches
    for pos, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_num_rows)):
        table = pa.Table.from_batches([batch])

        output_file_path = os.path.join(output_folder, f"chunk_{pos}.parquet")

        pq.write_table(table, output_file_path)
        print(
            f"Processed chunk number {pos} with {chunk_num_rows} rows, file saved at {output_file_path}"
        )


def split_parquet_file_auto(file_path: str, output_folder: str):
    chunk_num_rows = get_rows_per_chunk(file_path, "parquet")
    print(f"Chunk Num Rows : {chunk_num_rows}")
    split_parquet_file(file_path, output_folder, chunk_num_rows)
