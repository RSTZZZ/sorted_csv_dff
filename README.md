# Sorted CSV Difference

A simple Python script to sort two csv data into shared and unique rows (three files.)

```
python .\compare_csv.py {csv file 1} {csv file 2} {column index}
```

Example:

```sh
python .\compare_csv.py .\sample_files\file_4.csv .\sample_files\file_5.csv 2
```

or

```sh
python .\compare_csv.py .\sample_files\file_5.csv .\sample_files\file_4.csv 2
```

will return [both](.\sample_files\both.csv), [only_in_file_4](.\sample_files\only_in_file_4.csv) , [only_in_file_5](.\sample_files\only_in_file_5.csv)
