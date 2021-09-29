from kafka import KafkaProducer
from Devices import utils
from Devices.Bulb import *
from Devices.Camera import *
from Devices.Thermostat import *
import time


if __name__ == "__main__":
    choice = input("Insert the topic:\n[1] bulb_topic\n[2]camera_topic\n[3]term_topic\n")
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=utils.json_serializer)
    
    topic = None
    if choice == '1':
        topic = "bulb_topic"
        device = Bulb()
    elif choice == '2':
        topic = "camera_topic"
        device = Camera()
    else: 
        topic = "term_topic"
        device = Thermostat()

    while True:
        message = device.get_data()
        print("Producer message:\n", message)

        producer.send(topic=topic, value=message) \
            .add_callback(utils.on_send_success) \
            .add_errback(utils.on_send_error)

        time.sleep(random.randint(2, 4))
