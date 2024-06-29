import argparse
from argparse import Namespace


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

    args = parser.parse_args()
    return args
