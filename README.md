# littlechunks

### Overview
1) A Python based library to break large files into small files.
2) Currently supports csv , parquet files.
3) Can automatically determine the file size base on available memory and file size.
4) Maximum size of each chunk is set at 120 MB
### Installation
```python
>>> pip install littlechunks
```
### Use by importing into your script
1) Install littlechunks
2) To split a given csv file into smaller files of 1000 rows each
```python
>>> from littlechunks.csv_splitter import split_csv_file
>>> split_csv_file(input_file, output_folder, 1000, True)
```
3) To split a given csv file into smaller files whose file is determined by the program
```python
>>> from littlechunks.csv_splitter import split_csv_file_auto
>>> split_csv_file_auto(input_file, output_folder, True)
```
4) Similarly for parquet files
```python
>>> from littlechunks.parquet_splitter import split_parquet_file, split_parquet_file_auto
>>> split_parquet_file(input_file, output_folder, 1000)
>>> split_parquet_file_auto(input_file, output_folder)
```
### Requirements
1) python >= 3.11
2) packages = [ psutil, pyarrow ]

