#!/usr/bin/env python3
import dataclasses
import datetime
import json
import math
import os
import sys
import time
from collections import OrderedDict

import paho.mqtt.client as mqtt
import pika
import requests

# TODO: Remove global variables.
settings = None
stats = {"msgcount": 0}


@dataclasses.dataclass
class Settings:
    amqp_uri: str
    amqp_queue: str
    mqtt_host: str
    mqtt_port: int
    mqtt_topic: str
    mqtt_qos: int

    @staticmethod
    def load(filepath: str):
        with open(filepath, "r") as f:
            config = json.load(f)
            settings = Settings(**config)
            if "CLOUDAMQP_URL" in os.environ:
                settings.amqp_uri = os.environ["CLOUDAMQP_URL"]
            settings.mqtt_port = int(settings.mqtt_port)
            settings.mqtt_qos = int(settings.mqtt_qos)
            return settings


class InvalidMessage(Exception):
    pass


class Mqtt:
    def publish(self, message):
        client = mqtt.Client()
        client.connect(settings.mqtt_host, settings.mqtt_port)
        client.publish(settings.mqtt_topic, message, qos=settings.mqtt_qos)


def receive_handler(channel, method, properties, body):
    """
    Handler called on incoming messages.
    """

    stats["msgcount"] += 1

    # print("Received payload:", body)

    try:
        data = json.loads(body)
        submit_daq(data)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except InvalidMessage as ex:
        print(
            "WARNING: {}: Skipping invalid message: {}. {} messages so far".format(
                ex.__class__.__name__, ex, stats["msgcount"]
            )
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as ex:
        print("ERROR: {}: {}. {} messages so far".format(ex.__class__.__name__, ex, stats["msgcount"]))
        time.sleep(0.05)

    # Slow down script for debugging purposes
    # time.sleep(0.05)
    # time.sleep(0.1)
    # time.sleep(0.5)


def submit_daq(data):
    """
    Submit telemetry data to data acquisition system, using MQTT.
    """

    ndata = {}
    # Sanitize data
    for key, value in data.items():
        # print(str(key),str(value))
        # if key == 'timestamp':
        #    value = str(value)
        try:
            if not key == "timestamp":
                value = float(value)
                # print('converted to float')
        except InvalidMessage as ex:
            print(
                "WARNING: {}: Skipping invalid message: {}. {} messages so far".format(
                    ex.__class__.__name__, ex, stats["msgcount"]
                )
            )
        if type(value) is float and math.isnan(value):
            del data[key]
            print("deleting %s" % (key))
        # print('Key: %s : %s' % (key,value))
        ndata[key] = value

    mqtt = Mqtt()
    # print(json.dumps(ndata))
    mqtt.publish(json.dumps(ndata))


def run():

    settings = Settings.load(sys.argv[1])

    # Access the CLOUDAMQP_URL environment variable and parse it (fallback to localhost)
    print(f"Connecting to AMQP server at {settings.amqp_uri}")

    params = pika.URLParameters(settings.amqp_uri)

    bounces = 0

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.basic_consume(receive_handler, settings.amqp_queue)
            channel.start_consuming()

        except Exception as ex:
            print("ERROR: {}: {}. {} bounces so far".format(ex.__class__.__name__, ex, bounces))
            time.sleep(0.5)

        bounces += 1


if __name__ == "__main__":
    run()
