############
amqp-forward
############


*****
About
*****

Two programs to forward messages from AMQP to HTTP and MQTT.

- ``amqp-to-http.py``
- ``amqp-to-mqtt.py``


********
Synopsis
********

::

    # Setup
    python3 -m venv .venv
    source .venv/bin/activate
    pip install --requirement=requirements.txt

    # Invoke
    python amqp-to-http.py
    python amqp-to-mqtt.py amqp-to-mqtt.json
