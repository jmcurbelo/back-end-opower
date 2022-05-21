"""
This module contains the Write class which is used to write the data.
"""
from constants import country_col
from pyspark.sql import DataFrame


class Write:
    """
    This class contains the write_data method which is used to write the data in two differents mode append and
    overwrite.
    """

    def write_data(self, data_frame: DataFrame, repartition_number: int, write_mode: str, path: str) -> int:
        """
        This method is used to write the data in two differents mode append and overwrite.
        :param data_frame: A Spark DataFrame.
        :param repartition_number: An integer. This is the number of partition of your data. It is recommendable set
        this number equals to the number of core of your computer.
        :param write_mode: A string. This is the write mode which can be set equal to overwrite or append. This modes
        are the same write mode of write of Apache Spark overwrite and append.
        :param path: A string. The path where you want write the data.
        :return: An integer:

        0. The write was successful
        1. The write was unsuccessful
        """

        try:

            data_frame.repartition(repartition_number).write.mode(write_mode).partitionBy(country_col).parquet(path)

        except Exception as Ex:

            print('Failed to write DataFrame')

            print(Ex)

            return -1

        else:

            return 0
