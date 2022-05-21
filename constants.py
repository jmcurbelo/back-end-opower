from pyspark.sql.types import MapType, StringType, BooleanType, StructType, StructField, IntegerType, DecimalType

#  Constants
location = 'location'
parameter = 'parameter'
value = 'value'
country_col = 'country'
date_utc = 'date_utc'
coordinates_latitude = 'coordinates_latitude'
coordinates_longitude = 'coordinates_longitude'
name = 'name'

# Output paths
path_raw_data = './data/raw_data'
path_process_data = './data/process_data'

# Schema
schema = StructType([
            StructField("locationId", IntegerType()),
            StructField("location", StringType()),
            StructField("parameter", StringType()),
            StructField("value", DecimalType()),
            StructField("date", MapType(StringType(), StringType())),
            StructField("unit", StringType()),
            StructField("coordinates", MapType(StringType(), StringType())),
            StructField("country", StringType()),
            StructField("city", StringType()),
            StructField("isMobile", BooleanType()),
            StructField("isAnalysis", BooleanType()),
            StructField("entity", StringType()),
            StructField("sensorType", StringType())
        ])
