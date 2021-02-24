from collections import deque
from queuedwriters.csvWriteQueue import CSVQueueWriter
import sys
import os
import csv

from loguru import logger


class iter_wrapper(object):
    def __init__(self, it):
        self.it = it
        self._hasnext = None

    def __iter__(self): return self

    def next(self):
        if self._hasnext:
            result = self._thenext
        else:
            result = next(self.it)
        self._hasnext = None
        return result

    def has_next(self):
        if self._hasnext is None:
            try:
                self._thenext = next(self.it)
            except StopIteration:
                self._hasnext = False
            else:
                self._hasnext = True
        return self._hasnext


class CSVComparer():

    def __init__(self, csv_file_1: str, csv_file_2: str, same_file: str, only_in_file_1: str, only_in_file_2: str):
        self.csv_file_1 = csv_file_1
        self.csv_file_2 = csv_file_2
        self.same = CSVQueueWriter(
            "Both", same_file, buffer_size=100000, overwrite_file=True)
        self.file_1_only = CSVQueueWriter(
            "file_1_only", only_in_file_1, buffer_size=100000, overwrite_file=True)
        self.file_2_only = CSVQueueWriter(
            "file_2_only", only_in_file_2, buffer_size=100000, overwrite_file=True)

    def __deal_with_header(self, first_row, second_row):
        # Skip Header
        header = next(first_row)
        next(second_row)

        self.same.append(header)
        self.file_1_only.append(header)
        self.file_2_only.append(header)

    def __get_line_and_id(self, line, column_index: int):
        return (line, line[column_index] if column_index is not None else line)

    def __find_expected_line(self, it_line, expected_line, it: iter_wrapper, expected_it: iter_wrapper, it_result_queue: CSVQueueWriter, expected_result_queue: CSVQueueWriter, same_result_queue: CSVQueueWriter, column_index: int):

        # Add it_line to result queue since it is different
        it_result_queue.append(it_line)

        (it_line, it_id) = self.__get_line_and_id(it_line, column_index)
        (expected_line, expected_id) = self.__get_line_and_id(
            expected_line, column_index)

        # Loop through it
        while (it.has_next() and it_id < expected_id):

            # Get next line
            (it_line, it_id) = self.__get_line_and_id(it.next(), column_index)

            if (it_id < expected_id):
                it_result_queue.append(it_line)
            else:
                if (it_id == expected_id):
                    same_result_queue.append(expected_line)
                else:
                    self.__find_expected_line(
                        expected_line, it_line, expected_it, it, expected_result_queue, it_result_queue, same_result_queue, column_index)

        if (not it.has_next() or it_id < expected_id):
            expected_result_queue.append(expected_line)
            return

    def __compare_case_1(self, first_it, second_it, column_index: int = None):
        while (first_it.has_next() and second_it.has_next()):

            (first_line, first_id) = self.__get_line_and_id(
                first_it.next(), column_index)

            (second_line, second_id) = self.__get_line_and_id(
                second_it.next(), column_index)

            if (first_id == second_id):
                # Case 1.1: The lines are equal.
                self.same.append(first_line)
            else:
                if (first_id < second_id):
                    # Case 1.2: First has a row before.
                    self.__find_expected_line(
                        first_line, second_line, first_it, second_it, self.file_1_only, self.file_2_only, self.same, column_index)
                else:
                    # Case 1.3: Second has a row before.
                    #           Loop until we find the same one.
                    self.__find_expected_line(
                        second_line, first_line, second_it, first_it, self.file_2_only, self.file_1_only, self.same, column_index)

    def __add_to_it(self, it: iter_wrapper, result_queue: CSVQueueWriter):
        while it.has_next():
            result_queue.append(it.next())

    def __compare_case_2(self, first_it, second_it):

        if (first_it.has_next() and not second_it.has_next()):
            # Case 2: File 1 still has more lines:
            self.__add_to_it(first_it, self.file_1_only)

        elif (not first_it.has_next() and second_it.has_next()):
            # Case 3: File 2 still has more lines:
            self.__add_to_it(second_it, self.file_2_only)

    def compare(self, column_index: int):
        with open(self.csv_file_1, "r") as first:
            with open(self.csv_file_2, "r") as second:

                first_row = csv.reader(first)
                second_row = csv.reader(second)

                self.__deal_with_header(first_row, second_row)

                # Initialize iterator
                first_it = iter_wrapper(first_row)
                second_it = iter_wrapper(second_row)

                # Case 1: Both have a next:
                self.__compare_case_1(first_it, second_it, column_index)

                # Case 2: Finish the rest of the files
                self.__compare_case_2(first_it, second_it)

        self.same.flush()
        self.file_1_only.flush()
        self.file_2_only.flush()


if __name__ == "__main__":

    logger.add("test.log")

    first_file = sys.argv[1]
    second_file = sys.argv[2]
    column_index = int(sys.argv[3])

    stored_file_path = os.path.dirname(first_file)

    same_file = os.path.join(stored_file_path, "both.csv")
    only_in_first_file = os.path.join(
        stored_file_path, f"only_in_{os.path.basename(first_file)}")
    only_in_second_file = os.path.join(
        stored_file_path, f"only_in_{os.path.basename(second_file)}")

    csvcomparer = CSVComparer(
        first_file, second_file, same_file, only_in_first_file, only_in_second_file)

    csvcomparer.compare(column_index)
