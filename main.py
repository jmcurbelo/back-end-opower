"""
This module contains the logic of processing of this back-end process.
"""
from pyfiglet import Figlet
import openaq
from pyspark.sql import SparkSession
from read import ReadData
from write import Write
from constants import path_raw_data, path_process_data


class Main:
    """
    The Main class has a unique method run_process that launch all steps to obtain the data from OpenAQ API in two ways.
    """

    def run_program(self) -> int:
        """
        The run_program method do not take any parameter. The goal on this method is give us directions about two way
        in which we can get the data from OpenAQ API.

        You must be carefull when the program ask some data by console because a wrong input stop the program and give
        us a log of the error.

        The two methods for get data from  OpenAQ API are:
        1. country
        2. coordinates

        :return: An integer:

        0. The execution was successful
        1. The execution was unsuccessful
        """
        try:

            custom_fig = Figlet(font='slant')

            print(custom_fig.renderText('Back End Opower v1.0'))

            filter_by = str(
                input(
                    'Would you like filter the data by country/coordinates?\n'
                    '\ncountry => Filter the data by one country\n'
                    'coordinates => Filter the data by certain coordinates\n'))

            if filter_by == 'country':

                country_variable = str(input('Enter a country. Example MX\n'))

                parameter_list = str(input('Enter a list of parameter comma separated\nFor example: '
                                           'pm1,pm10,pm25,um010,um100\n')).split(',')

                api = openaq.OpenAQ(version='v2')

                spark = SparkSession.builder.getOrCreate()

                read = ReadData(api, spark)

                write = Write()

                local_mode = str(input('Would you like append the new data or overwrite the data that exist:\n'
                                       'append\noverwrite\n'))

                raw_data = read.get_data_by_country(country_variable, parameter_list)

                write.write_data(raw_data, 3, local_mode, path_raw_data)

                process_data = read.process_data(raw_data)

                write.write_data(process_data, 3, local_mode, path_process_data)

                print('Raw Data')

                spark.read.parquet(path_raw_data).show(50, truncate=False)

                print('Process Data')

                spark.read.parquet(path_process_data).show(50, truncate=False)

            elif filter_by == 'coordinates':

                coordinates = str(input('Enter the center point (lat, long) used to get measurements within a certain '
                                        'area. (Ex: 51.509865,-0.118092)\n'))

                radius = int(input('Enter the radius (in meters) used to get measurements\n'))

                parameter_list = str(input('Enter a list of parameter comma separated\nFor example: '
                                           'pm25,o3,no2\n')).split(',')

                api = openaq.OpenAQ(version='v2')

                spark = SparkSession.builder.getOrCreate()

                read = ReadData(api, spark)

                write = Write()

                local_mode = str(input('Would you like append the new data or overwrite the data that exist:\n'
                                       'append\noverwrite\n'))

                raw_data = read.get_data_by_coordinates(coordinates, radius, parameter_list)

                write.write_data(raw_data, 3, local_mode, path_raw_data)

                process_data = read.process_data(raw_data)

                write.write_data(process_data, 3, local_mode, path_process_data)

                print('Raw Data')

                spark.read.parquet(path_raw_data).show(50, truncate=False)

                print('Process Data')

                spark.read.parquet(path_process_data).show(50, truncate=False)

            else:

                print('Wrong option, please enter the word country or coordinates.')

        except Exception as Ex:

            print('Something gone wrong, please try again')

            print(Ex)

            return -1

        else:

            return 0


if __name__ == "__main__":
    main = Main()
    main.run_program()
