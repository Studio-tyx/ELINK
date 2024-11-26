import random
import time

from paho.mqtt import client as mqtt_client

class MQTTPublish:
    def __init__(self):
        self.broker = '10.192.232.252'
        self.port = 1883
        self.topic = "/python/mqtt"
        self.client_id = f'python-mqtt-{random.randint(0, 1000)}'

    def connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            pass
            '''
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
            '''

        client = mqtt_client.Client(self.client_id)
        client.on_connect = on_connect
        client.connect(self.broker, self.port)
        return client

    def publish(self, _input):
        publish_msg = ''
        if len(_input) == 4:
            publish_msg = _input[3]
        else:
            publish_msg = _input[4]

        client = self.connect_mqtt()
        result = client.publish(self.topic, bytearray(publish_msg.encode('utf-8')))
        client.disconnect()
        '''
        status = result[0]
        if status == 0:
            print(f"Send msg to topic `{self.topic}`")
        else:
            print(f"Failed to send message to topic {self.topic}")
        '''
        return _input
