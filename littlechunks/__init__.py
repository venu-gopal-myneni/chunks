from littlechunks.csv import split_csv_file
from littlechunks.utils import get_args


def main():
    args = get_args()
    split_csv_file(args.input_file, args.output_folder, args.chunk_size, args.header)
