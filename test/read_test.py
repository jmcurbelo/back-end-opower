import unittest
import openaq
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from read import ReadData


class ReadTest(unittest.TestCase):

    def setUp(self) -> None:
        self.api = openaq.OpenAQ(version='v2')

        self.spark = SparkSession.builder.getOrCreate()

        self.sc = self.spark.sparkContext

        self.input = ReadData(self.api, self.spark)

    def test_get_data_by_country(self):
        self.assertIsInstance(
            self.input.get_data_by_country('MX', ['pm1', 'pm10', 'pm25', 'um010', 'um100']), DataFrame, 'Should be a DataFrame')

    def test_get_data_by_coordinates(self):
        self.assertIsInstance(
            self.input.get_data_by_coordinates('51.509865,-0.118092', 3000, ['pm25', 'o3', 'no2']), DataFrame, 'Should be a DataFrame'
        )

    def test_process_data(self):

        country_df = self.input.get_data_by_country('MX', ['pm1', 'pm10', 'pm25', 'um010', 'um100'])

        coordinates_df = self.input.get_data_by_coordinates('51.509865,-0.118092', 3000, ['pm25', 'o3', 'no2'])

        self.assertIsInstance(self.input.process_data(country_df), DataFrame, 'Should be a DataFrame')

        self.assertIsInstance(self.input.process_data(coordinates_df), DataFrame, 'Should be a DataFrame')


if __name__ == '__main__':
    unittest.main()
    