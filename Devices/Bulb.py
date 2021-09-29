import random
import barnum
from pyspark.sql.types import *


class Bulb:
    def __init__(self):
        self.date = None
        self.time = None
        self.status = None
        self.brightness = None
        self.mode = None

    def __read_time(self):
        date_time = barnum.create_date(past=True, max_years_past=1, max_years_future=1)
        t = date_time.time()
        d = date_time.year
        self.time = t
        self.date = d
        return d, t

    def read_status(self): 
        s = random.choice(['on', 'off'])
        self.status = s
        return s
    
    def read_brightness(self):
        b = random.uniform(0, 100)
        self.brightness = b 
        return b  # brightness in percentage
    
    def read_mode(self):
        m = random.choice(['cold', 'hot'])
        self.mode = m
        return m
    
    def get_data(self):
        date, time = self.__read_time()
        return {
                "date": date,
                "time": str(time),
                "status": self.read_status(), 
                "brightness": self.read_brightness(), 
                "mode": self.read_mode()
                }
    
    @staticmethod
    def get_schema():
        schema = StructType() \
            .add("date", IntegerType()) \
            .add("time", StringType()) \
            .add("status", StringType()) \
            .add("brightness", DoubleType()) \
            .add("mode", StringType())
        return schema
