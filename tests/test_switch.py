import asyncio
import json
import threading
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode
from viam.proto.app.robot import ComponentConfig
from viam.utils import dict_to_struct

from zigbee2mqtt_bridge import switch as switch_mod
from zigbee2mqtt_bridge.switch import OnOffSwitch

STATE_TOPIC = "zigbee2mqtt/mixer-plug"


def make_config(**attrs) -> ComponentConfig:
    base = {"mqtt_server": "mqtt://localhost", "device": "mixer-plug", "confirm_timeout_ms": 200}
    base.update(attrs)
    return ComponentConfig(name="mixer-power", attributes=dict_to_struct(base))


class FakeClient:
    """Stands in for paho; replies from a separate thread like paho's network loop does."""

    def __init__(self, callbacks, *, respond: bool = True) -> None:
        self.callbacks = callbacks
        self.respond = respond
        self.published: list[tuple[str, dict]] = []
        self.publish_rc = mqtt.MQTT_ERR_SUCCESS

    def connect(self) -> None:
        rc = ReasonCode(PacketTypes.CONNACK, "Success")
        self.callbacks["on_connect"](self, None, None, rc, None)

    def report(self, payload: dict | bytes, topic: str = STATE_TOPIC) -> None:
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        msg = SimpleNamespace(topic=topic, payload=raw)
        t = threading.Thread(target=self.callbacks["on_message"], args=(self, None, msg))
        t.start()
        t.join()

    def subscribe(self, topic, qos=0) -> None:
        pass

    def publish(self, topic, payload, qos=0):
        body = json.loads(payload)
        self.published.append((topic, body))
        if self.publish_rc == mqtt.MQTT_ERR_SUCCESS and self.respond:
            if topic.endswith("/set"):
                self.report({"state": body["state"], "power": 0})
            elif topic.endswith("/get"):
                self.report({"state": "OFF"})
        return SimpleNamespace(rc=self.publish_rc)

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass


@pytest.fixture
def make_switch(monkeypatch):
    clients: list[FakeClient] = []

    def build(*, respond: bool = True, connect: bool = True, **attrs):
        def fake_start_client(attrs_, **callbacks):
            client = FakeClient(callbacks, respond=respond)
            clients.append(client)
            return client

        monkeypatch.setattr(switch_mod, "start_client", fake_start_client)
        sw = OnOffSwitch.new(make_config(**attrs), {})
        if connect:
            clients[-1].connect()
        return sw, clients[-1]

    return build


async def settle() -> None:
    await asyncio.sleep(0)
    await asyncio.sleep(0)


async def test_set_position_publishes_and_waits_for_confirmation(make_switch):
    sw, client = make_switch()
    client.published.clear()

    await sw.set_position(1)
    await sw.set_position(0)

    assert client.published == [
        (f"{STATE_TOPIC}/set", {"state": "ON"}),
        (f"{STATE_TOPIC}/set", {"state": "OFF"}),
    ]
    assert await sw.get_position() == 0


async def test_set_position_times_out_when_device_silent(make_switch):
    sw, _ = make_switch(respond=False)

    with pytest.raises(TimeoutError, match="did not report state ON"):
        await sw.set_position(1)
    assert sw._waiters == []


async def test_set_position_ignores_other_states_while_waiting(make_switch):
    sw, client = make_switch(respond=False)

    task = asyncio.create_task(sw.set_position(1))
    await settle()
    client.report({"state": "OFF"})
    await settle()
    assert not task.done()
    client.report({"state": "ON"})
    await task


async def test_set_position_fails_fast_when_disconnected(make_switch):
    sw, client = make_switch(connect=False)

    with pytest.raises(ConnectionError, match="not connected"):
        await sw.set_position(1)
    assert client.published == []


async def test_set_position_surfaces_publish_errors(make_switch):
    sw, client = make_switch()
    client.publish_rc = mqtt.MQTT_ERR_NO_CONN

    with pytest.raises(ConnectionError, match="publish"):
        await sw.set_position(1)


@pytest.mark.parametrize("position", [-1, 2])
async def test_set_position_rejects_out_of_range(make_switch, position):
    sw, _ = make_switch()
    with pytest.raises(ValueError, match="0 \\(off\\) or 1 \\(on\\)"):
        await sw.set_position(position)


async def test_connect_requests_current_state(make_switch):
    sw, client = make_switch()
    await settle()

    assert client.published == [(f"{STATE_TOPIC}/get", {"state": ""})]
    assert await sw.get_position() == 0


async def test_get_position_tracks_unsolicited_reports(make_switch):
    sw, client = make_switch()
    client.report({"state": "ON", "power": 120.5})
    await settle()

    assert await sw.get_position() == 1


async def test_get_position_raises_when_state_unknown(make_switch):
    sw, _ = make_switch(respond=False)
    with pytest.raises(TimeoutError, match="did not report state any"):
        await sw.get_position()


async def test_ignores_other_topics_and_bad_payloads(make_switch):
    sw, client = make_switch(respond=False)
    client.report({"state": "ON"}, topic="zigbee2mqtt/other-plug")
    client.report(b"not json")
    client.report({"state": "TOGGLE"})
    client.report({"power": 3})
    await settle()

    assert sw._state is None


async def test_number_of_positions(make_switch):
    sw, _ = make_switch()
    assert await sw.get_number_of_positions() == (2, ["off", "on"])


@pytest.mark.parametrize(
    ("attrs", "match"),
    [
        ({"device": ""}, "`device` is required"),
        ({"device": "a/b"}, "`device` is required"),
        ({"mqtt_server": ""}, "`mqtt_server` is required"),
        ({"confirm_timeout_ms": 0}, "`confirm_timeout_ms`"),
        ({"topic_prefix": ""}, "`topic_prefix`"),
    ],
)
def test_validate_rejects_bad_config(attrs, match):
    with pytest.raises(ValueError, match=match):
        OnOffSwitch.validate(make_config(**attrs))


def test_validate_has_no_dependencies():
    assert OnOffSwitch.validate(make_config()) == ([], [])
