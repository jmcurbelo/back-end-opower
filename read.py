"""
This module contains the ReadData class which is used to read and process the data from the OpenAQ API.
"""
from pyspark.sql.functions import col, regexp_replace, from_json, avg, map_values
from constants import *
from openaq import OpenAQ
from pyspark.sql import DataFrame


class ReadData:
    """
    This class contains three methods for read and process the data from the OpenAQ API.
    """

    def __init__(self, api: OpenAQ, spark: DataFrame) -> None:

        self.api = api

        self.spark = spark

        self.sc = spark.sparkContext

    def get_data_by_country(self, country: str, parameters: list) -> DataFrame:
        """
        This method read the data from the OpenAQ API and filter them by country and a given list of parameters.
        :param country: This a string of length two and the two letters must be capitalize. Example US.
        :param parameters: This a comma separated list which contains the parameters that we want in our data.
        :return: A Spark DataFrame
        """

        try:
            status, resp = self.api.measurements(country=country)

            df = self.spark.read.json(self.sc.parallelize(resp['results'])).withColumnRenamed('_corrupt_record',
                                                                                              'value').withColumn(
                'value', regexp_replace(col('value'), "'", '"')
            ).withColumn('value', regexp_replace(col('value'), 'False', 'false')).withColumn('value',
                                                                                             regexp_replace(
                                                                                                 col('value'),
                                                                                                 'None', 'null'))

            df1 = df.select(from_json(df.value, schema, {"mode": "PERMISSIVE"}).alias("json"))

            print('Json Data:')

            df1.show(truncate=False)

            return df1.select(
                col("json.locationId"),
                col("json.location"),
                col("json.parameter"),
                col("json.value"),
                col("json.date"),
                col("json.unit"),
                col("json.coordinates"),
                col("json.country"),
                col("json.city"),
                col("json.isMobile"),
                col("json.isAnalysis"),
                col("json.entity"),
                col("json.sensorType")
            ).filter(col(parameter).isin(parameters))

        except Exception as Ex:

            print('Something gone wrong, please try again')

            print(Ex)

    def get_data_by_coordinates(self, coordinate: str, radius: int, parameters: list) -> DataFrame:
        """
        This method reads the data from the OpenAQ API and filters it by coordinates. In addition, you must indicate
        the radius with which the data will be filtered and the parameters that you want to include in the output.
        :param coordinate: A string in the form 51.509865,-0.118092. The first position indicating the latitude and the
        second indicating the longitude.
        :param radius: A integer indicating the radius. This must be expressed in meters.
        :param parameters: This a comma separated list which contains the parameters that we want in our data.
        :return: A Spark DataFrame
        """

        try:

            status, resp = self.api.measurements(coordinates=coordinate, radius=radius)

            df = self.spark.read.json(self.sc.parallelize(resp['results'])
                                      ).withColumnRenamed('_corrupt_record', 'value').withColumn(
                'value', regexp_replace(col('value'), "'", '"')
            ).withColumn('value',
                         regexp_replace(col('value'), 'False', 'false')
                         ).withColumn('value', regexp_replace(col('value'), 'None', 'null'))

            df1 = df.select(from_json(df.value, schema, {"mode": "FAILFAST"}).alias("json"))

            print('Json Data:')

            df1.show(truncate=False)

            return df1.select(
                col("json.locationId"),
                col("json.location"),
                col("json.parameter"),
                col("json.value"),
                col("json.date"),
                col("json.unit"),
                col("json.coordinates"),
                col("json.country"),
                col("json.city"),
                col("json.isMobile"),
                col("json.isAnalysis"),
                col("json.entity"),
                col("json.sensorType")
            ).filter(col(parameter).isin(parameters))

        except Exception as Ex:

            print('Something gone wrong, please try again')

            print(Ex)

    def process_data(self, data_frame: DataFrame) -> DataFrame:
        """
        This method receives the raw data and prepare it for use to build a heatmap.
        :param data_frame: A Spark DataFrame.
        :return: A Spark DataFrame.
        """

        try:
            return data_frame.select(
                col('locationId'),
                col('location'),
                col('parameter'),
                col('value').alias('parameter_value'),
                col('unit'),
                col('city'),
                col('entity'),
                col('country'),
                map_values(col('coordinates')).alias('coordinates')) \
                .groupBy('locationId', 'location', 'city', 'entity', 'country', 'coordinates') \
                .pivot('parameter').agg(avg('parameter_value'))

        except Exception as Ex:

            print('Something gone wrong, please try again')

            print(Ex)
