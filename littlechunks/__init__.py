from littlechunks.csv_splitter import split_csv_file, split_csv_file_auto
from littlechunks.parquet_splitter import split_parquet_file, split_parquet_file_auto
from littlechunks.utils import get_args


def main():
    args = get_args()
    input_file = args.input_file
    auto = args.auto
    if input_file.endswith(".csv"):
        file_type = "csv"
    elif input_file.endswith(".parquet"):
        file_type = "parquet"
    else:
        raise ValueError(f"Unknown file type : {input_file}")
    if file_type == "csv":
        if auto:
            split_csv_file_auto(args.input_file, args.output_folder, args.header)
        else:
            split_csv_file(
                args.input_file, args.output_folder, args.chunk_size, args.header
            )
    if file_type == "parquet":
        if auto:
            split_parquet_file(args.input_file, args.output_folder, args.chunk_size)
        else:
            split_parquet_file_auto(args.input_file, args.output_folder)
