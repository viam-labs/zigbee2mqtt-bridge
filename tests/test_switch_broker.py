"""End-to-end against a real MQTT broker with a simulated zigbee2mqtt plug.

Runs only when MQTT_TEST_SERVER is set, e.g.:
    docker run --rm -d -p 127.0.0.1:1883:1883 eclipse-mosquitto:2 \
        mosquitto -c /mosquitto-no-auth.conf
    MQTT_TEST_SERVER=mqtt://localhost:1883 pytest tests/test_switch_broker.py
"""

import asyncio
import json
import os
import uuid

import paho.mqtt.client as mqtt
import pytest
from viam.proto.app.robot import ComponentConfig
from viam.utils import dict_to_struct

from zigbee2mqtt_bridge.switch import OnOffSwitch

SERVER = os.environ.get("MQTT_TEST_SERVER")
pytestmark = pytest.mark.skipif(not SERVER, reason="MQTT_TEST_SERVER not set")


class SimulatedPlug:
    """Behaves like zigbee2mqtt for one device: echoes /set and answers /get."""

    def __init__(self, prefix: str, device: str) -> None:
        self.topic = f"{prefix}/{device}"
        self.state = "OFF"
        self.online = True
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_message = self._on_message
        host, port = SERVER.removeprefix("mqtt://").split(":")
        self.client.connect(host, int(port))
        self.client.subscribe(f"{self.topic}/+", qos=1)
        self.client.loop_start()

    def _on_message(self, client, userdata, msg) -> None:
        if not self.online:
            return
        if msg.topic.endswith("/set"):
            self.state = json.loads(msg.payload)["state"]
        self.client.publish(self.topic, json.dumps({"state": self.state, "power": 0}), qos=1)

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


async def wait_connected(sw: OnOffSwitch) -> None:
    for _ in range(50):
        if sw._connected:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("switch never connected to the broker")


async def test_round_trip_through_broker():
    prefix = f"z2m-test-{uuid.uuid4().hex[:8]}"
    plug = SimulatedPlug(prefix, "mixer-plug")
    attrs = {
        "mqtt_server": SERVER,
        "topic_prefix": prefix,
        "device": "mixer-plug",
        "confirm_timeout_ms": 2000,
    }
    sw = OnOffSwitch.new(ComponentConfig(name="mixer-power", attributes=dict_to_struct(attrs)), {})
    try:
        await wait_connected(sw)
        assert await sw.get_position() == 0

        await sw.set_position(1)
        assert plug.state == "ON"
        assert await sw.get_position() == 1

        await sw.set_position(0)
        assert plug.state == "OFF"

        plug.online = False
        with pytest.raises(TimeoutError):
            await sw.set_position(1)
    finally:
        await sw.close()
        plug.stop()
