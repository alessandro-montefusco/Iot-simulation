import random
import barnum
from pyspark.sql.types import *


class Camera:
    def __init__(self):
        self.time = None
        self.date = None
        self.detected = None
        self.info = None

    def __read_datetime(self):
        date_time = barnum.create_date(past=True, max_years_past=1, max_years_future=1)
        t = date_time.time()
        d = date_time.year
        self.time = t
        self.date = d
        return d, t

    def __read_detected(self):
        d = random.choice(['human', 'animal', 'car', 'bus', 'camion'])
        self.detected = d
        return d
    
    def __read_info(self):
        i = barnum.create_sentence(min=10, max=20)
        self.info = i
        return i
    
    def get_data(self):
        date, time = self.__read_datetime()
        return {
                "date": date,
                "time": str(time),
                "detected": self.__read_detected(), 
                "info": self.__read_info()
                }
    
    @staticmethod
    def get_schema():
        schema = StructType() \
            .add("date", IntegerType()) \
            .add("time", StringType()) \
            .add("detected", StringType()) \
            .add("info", StringType())
        return schema
