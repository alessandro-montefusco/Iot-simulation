import random
import barnum
from pyspark.sql.types import *


class Thermostat:
    def __init__(self):
        self.date = None
        self.time = None
        self.temperature = None
        self.humidity = None
        self.pressure = None
    
    def __read_datetime(self):
        date_time = barnum.create_date(past=True, max_years_past=1, max_years_future=1)
        t = date_time.time()
        d = date_time.year
        self.time = t
        self.date = d
        return d, t
    
    def __read_temperature(self):
        t = round(random.uniform(0, 40), 2)
        self.temperature = t
        return t  # temperature in °C
    
    def __read_humidity(self): 
        h = round(random.uniform(0, 100), 2)
        self.humidity = h
        return h  # humidity in percentage

    def __read_pressure(self): 
        p = round(random.uniform(870, 1094), 2)
        self.pressure = p
        return p  # pressure in hPA

    def get_data(self):
        date, time = self.__read_datetime()
        return {
                "date": date,
                "time": str(time),
                "temperature": self.__read_temperature(), 
                "humidity": self.__read_humidity(), 
                "pressure": self.__read_pressure()
                }
    
    @staticmethod
    def get_schema():
        schema = StructType() \
            .add("date", IntegerType()) \
            .add("time", StringType()) \
            .add("temperature", DoubleType()) \
            .add("humidity", DoubleType()) \
            .add("pressure", DoubleType())
        return schema
