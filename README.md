# back-end-opower

## How install all necessary dependencies?

Once you have cloned the repository it is recommended to 
create a virtual environment and install the dependencies 
using the `requirements.txt` file. For this you can use the 
command `pip install -r requirements.txt`.

## Project structure

The project uses the Apache Spark framework and Python.
The project is made up of a series of `.py` files and a test 
folder that contains the unit tests for the methods used.

The `main.py` file contains the main development logic. Here 
you will find the `run_program` function which is used to run 
the program. Once you have executed this function, you will 
only have to follow the instructions that the program itself 
will request.

The `constants.py`, `read.py`, and `write.py` files are required 
packages that complement the workings of the `main.py` file. In
these files you will find the reading, processing and 
writing logic of the program.

The test folder contains the unit tests corresponding to 
each of the modules, classes and methods found in them.

## How does the program work?

Once you execute the `run_program` method of the `main.py` file
you will be prompted to specify the method by which you want 
to filter the data coming from the OpenAQ API. There are two 
filtering methods implemented:

* **By country:** This filtering method allows you to filter 
the information coming from the OpenAQ API by the desired 
country. You should note that the required input here is 
two uppercase characters representing the country in 
correspondence with the **ISO 3166-1 alpha-2** standard. 
Once you have entered the country you will be prompted 
to enter the parameters you want to be used in the 
output data.

* **By coordinates:** This filtering method allows you to 
filter the information coming from the OpenAQ API by 
certain coordinates. You should note that the required 
input here is the desired coordinates. Once you have 
entered the coordinates, you will be asked for the radius 
you want to use. You must take into account that this 
radius is the distance from the coordinate point to the 
edge of the circumference that will be used to filter the 
information. Finally, you will be asked to enter the
parameters you want to be used in the output data.

For both of the above methods you will be prompted to 
specify the mode of writing the data. You should note 
that the method you select will be used to write both 
raw data and process data (see explanation of raw data 
and process data below).

* **append mode:** This writing mode adds the obtained information
to the previously existing one.

* **overwrite mode:** This writing mode will erase all previously
existing data and write the new data.

## How does the data is written?

The data is written to two different addresses inside 
the data folder:

* The first `raw_data` is a Spark DataFrame that contains more 
information but is not fully processed to be used by the 
front to create the heatmap.

* The second `process_data` is a Spark DataFrame that contains 
a little less information but in our opinion is much 
easier to use and contains the main metrics that can be 
used for the construction of the heatmap. This data 
carries an extra layer of processing.

In both cases, from the raw phase, the data has a schema 
that we have built to guarantee its consistency. In 
addition, the data is partitioned by country, which 
offers the possibility of performing a fast and efficient 
filter in case you want to read the information to build 
the front.

The idea of ​​having the data in two different paths 
is to keep the information in two different states and 
give the development team the opportunity to have a master 
layer of the data (process data) and a raw layer 
(raw data) in case they wish to implement other metrics 
or simply as a backup measure of the information.
