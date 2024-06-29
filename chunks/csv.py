import csv
from pathlib import Path


def list_of_lists_to_csv(data, file_path, newline, encoding):
    """

    :param data:
    :param file_path:
    :param newline:
    :param encoding:
    :return:
    """
    with open(file_path, mode="w", newline=newline, encoding=encoding) as file:
        writer = csv.writer(file)
        writer.writerows(data)


def csv_row_generator(
    file_path,
    chunk_size: int,
    header: bool,
    delimiter: str,
    newline: str,
    encoding: str,
):
    """

    :param file_path:
    :param chunk_size:
    :param header:
    :param delimiter:
    :param newline:
    :param encoding:
    :return:
    """
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


def split_csv_file(
    input_file: str,
    output_folder: str,
    chunk_size: int,
    header: bool,
    delimiter: str = ",",
    newline: str = "",
    encoding: str = "utf-8",
):
    """

    :param input_file:
    :param output_folder:
    :param chunk_size:
    :param header:
    :param delimiter:
    :param newline:
    :param encoding:
    :return:
    """
    for pos, chunk in enumerate(
        csv_row_generator(input_file, chunk_size, header, delimiter, newline, encoding)
    ):
        list_of_lists_to_csv(
            chunk, Path(output_folder, f"chunk_{pos}.csv"), newline, encoding
        )
        print(f"Processed chunk number {pos} with {len(chunk)} rows")


if __name__ == "__main__":
    # Example usage
    input_file = r"C:\Users\mailv\projects\data\hmda_2017_nationwide_all-records_labels\hmda_2017_nationwide_all-records_labels.csv"
    input_file =r"C:\Users\mailv\projects\data\email-password-recovery-code.csv"
    split_csv_file(input_file, r"C:\Users\mailv\projects\data\chunks", 3, True)
