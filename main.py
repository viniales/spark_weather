import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType


def get_weather(city):
    api_key = "aabcce4f2010458c501e4f8230f90fae"
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    data = response.json()
    if "main" in data and "temp" in data["main"]:
        return round(float(data["main"]["temp"]) - 273.15, 2), data["name"], round(
            float(data["main"]["temp_max"]) - 273.15, 2)
    else:
        return None, None


spark = SparkSession.builder \
    .appName("TemperatureStreaming") \
    .getOrCreate()

# Define the schema for temperature data
schema = DoubleType()

weather_stream = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load() \
    .withColumn("temperature", lit(get_weather("Paris")[0])) \
    .withColumn("city", lit(get_weather("Paris")[1])) \
    .withColumn("temp_max", lit(get_weather("Paris")[2]))

query = weather_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
