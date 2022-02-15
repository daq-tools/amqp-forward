#!/usr/bin/env python3
import dataclasses
import json
import math
import os
import time

import pika
import requests

# TODO: Remove global variables.
settings = None


@dataclasses.dataclass
class Settings:
    amqp_uri: str
    amqp_queue: str
    http_uri: str


def receive_handler(channel, method, properties, body):
    """
    Handler called on incoming messages.
    """

    # print("Received payload:", body)

    try:
        data = json.loads(body)
        submit_daq(data)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as ex:
        print("ERROR: {}: {}".format(ex.__class__.__name__, ex))
        time.sleep(0.05)


def submit_daq(data):
    """
    Submit telemetry data to data acquisition system, using HTTP.
    """

    # Sanitize data
    for key, value in data.items():
        # print(key, value)
        if value == "nan" or (type(value) is float and math.isnan(value)):
            del data[key]

    # print("Submitting to DAQ host:", data)

    # Compute channel name by applying some heuristics.
    # TODO: Put this into another function.

    # Submit GPS data
    if "latitude" in data:
        channel = "foobar/solarbox/gps"

    # Submit epsolar data
    elif "batterie_volt" in data:
        channel = "foobar/solarbox/epsolar"

    # Submit epsolar data
    elif "humidity" in data:
        channel = "foobar/solarbox/bm280"

    else:
        raise ValueError("Unknown message: {}".format(data))

    request = requests.post(settings.http_uri.format(channel=channel), json=data)
    request.raise_for_status()


def run():

    # Get AMQP broker address, either from settings or from `CLOUDAMQP_URL` environment variable.
    amqp_uri = os.environ.get("CLOUDAMQP_URL", settings.amqp_uri)
    # print("Connecting to AMQP server at {}".format(url))

    # TODO: Gracefully reconnect to AMQP broker.
    params = pika.URLParameters(amqp_uri)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()  # start a channel
    # channel.queue_declare(queue='gpslogger') # Declare a queue

    # Set up subscription on the queue.
    # channel.basic_consume(receive_handler, queue=settings.amqp_queue, no_ack=True)
    channel.basic_consume(receive_handler, queue=settings.amqp_queue, exclusive=True)

    # Start consuming (blocking).
    channel.start_consuming()
    connection.close()


if __name__ == "__main__":
    settings = Settings(
        amqp_uri="amqp://user:pass@localhost:5672",
        amqp_queue="queuename",
        http_uri="https://daq.example.org/api/workbench/{channel}/data",
    )
    run()
