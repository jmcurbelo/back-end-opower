import unittest
import openaq
from pyspark.sql import SparkSession
from read import ReadData
from write import Write


class WriteTest(unittest.TestCase):

    def setUp(self) -> None:

        self.api = openaq.OpenAQ(version='v2')

        self.spark = SparkSession.builder.getOrCreate()

        self.sc = self.spark.sparkContext

        self.input = ReadData(self.api, self.spark)

        self.write = Write()

    def test_write_data(self):

        country_df = self.input.get_data_by_country('MX', ['pm1', 'pm10', 'pm25', 'um010', 'um100'])

        self.assertEqual(
            self.write.write_data(country_df, 3, 'overwrite', './output'), 0, "Should be write the data"
        )

        self.assertEqual(
            self.write.write_data(country_df, 3, 'write', './output'), -1, "Should be fail because the write mode is wrong"
        )

