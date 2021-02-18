# Sorted CSV Difference

A simple Python script to sort two csv data into shared and unique rows (three files.)

```
python .\compare_csv.py {csv file 1} {csv file 2} {column index}
```

Example:

```sh
python .\compare_csv.py .\file_4.csv .\file_5.csv 2
```

or

```sh
python .\compare_csv.py .\file_5.csv .\file_4.csv 2
```

will return [both](.\both.csv), [only_in_file_4](.\only_in_file_4.csv) , [only_in_file_5](.\only_in_file_5.csv)
