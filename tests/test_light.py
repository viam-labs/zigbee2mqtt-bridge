import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode
from viam.proto.app.robot import ComponentConfig
from viam.utils import dict_to_struct

from zigbee2mqtt_bridge import light as light_mod
from zigbee2mqtt_bridge.light import HEALTH_MESSAGES, Light

STATE_TOPIC = "zigbee2mqtt/bedroom-lamp"
AVAIL_TOPIC = f"{STATE_TOPIC}/availability"


def make_config(name: str = "bedroom-lamp", **attrs) -> ComponentConfig:
    base = {
        "mqtt_server": "mqtt://localhost",
        "device": "bedroom-lamp",
        "confirm_timeout_ms": 200,
    }
    base.update(attrs)
    return ComponentConfig(name=name, attributes=dict_to_struct(base))


class FakeClient:
    def __init__(self, callbacks, *, respond: bool = True) -> None:
        self.callbacks = callbacks
        self.respond = respond
        self.published: list[tuple[str, dict]] = []
        self.publish_rc = mqtt.MQTT_ERR_SUCCESS

    def connect(self) -> None:
        rc = ReasonCode(PacketTypes.CONNACK, "Success")
        self.callbacks["on_connect"](self, None, None, rc, None)

    def report(self, payload, topic: str = STATE_TOPIC) -> None:
        if isinstance(payload, bytes):
            raw = payload
        elif isinstance(payload, str):
            raw = payload.encode()
        else:
            raw = json.dumps(payload).encode()
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
                echo = {"state": "ON"}
                for k in ("brightness", "color_temp"):
                    if k in body and body[k] != "":
                        echo[k] = body[k]
                self.report(echo)
            elif topic.endswith("/get"):
                self.report({"state": "ON", "brightness": 128, "color_temp": 300})
        return SimpleNamespace(rc=self.publish_rc)

    def loop_stop(self) -> None:
        pass

    def disconnect(self) -> None:
        pass


@pytest.fixture
def make_light(monkeypatch, tmp_path):
    clients: list[FakeClient] = []

    def build(*, respond: bool = True, connect: bool = True, name: str = "bedroom-lamp", **attrs):
        state_path = attrs.pop("state_path", None) or str(tmp_path / f"{name}.json")
        cfg = make_config(name=name, state_path=state_path, **attrs)

        def fake_start_client(attrs_, **callbacks):
            client = FakeClient(callbacks, respond=respond)
            clients.append(client)
            return client

        monkeypatch.setattr(light_mod, "start_client", fake_start_client)
        lamp = Light.new(cfg, {})
        if connect:
            clients[-1].connect()
            # on_connect publishes /get and power_on_behavior — flush and discard
            # so tests can assert on just their own publishes.
            clients[-1].published.clear()
        return lamp, clients[-1], state_path

    return build


async def settle() -> None:
    await asyncio.sleep(0)
    await asyncio.sleep(0)


def test_validate_returns_tuple_form():
    assert Light.validate(make_config()) == ([], [])


@pytest.mark.parametrize(
    ("attrs", "match"),
    [
        ({"device": ""}, "`device` is required"),
        ({"device": "a/b"}, "`device` is required"),
        ({"mqtt_server": ""}, "`mqtt_server` is required"),
        ({"confirm_timeout_ms": 0}, "`confirm_timeout_ms`"),
        ({"topic_prefix": ""}, "`topic_prefix`"),
        ({"power_on_behavior": "bananas"}, "`power_on_behavior`"),
    ],
)
def test_validate_rejects_bad_config(attrs, match):
    with pytest.raises(ValueError, match=match):
        Light.validate(make_config(**attrs))


def test_validate_rejects_malformed_seed_schedule():
    bad = {
        "time": "06:30",
        "duration_s": -1,
        "start_brightness_pct": 0,
        "start_color_temp_k": 2200,
        "target_brightness_pct": 80,
        "target_color_temp_k": 4000,
    }
    with pytest.raises(ValueError, match="duration_s"):
        Light.validate(make_config(schedules=[bad]))


async def test_set_brightness_publishes_zb_integer_and_waits(make_light):
    lamp, client, _ = make_light()
    await lamp.do_command({"command": "set", "brightness_pct": 50})
    assert len(client.published) == 1
    topic, body = client.published[0]
    assert topic == f"{STATE_TOPIC}/set"
    # Gamma 2.2: 50% perceived → ~22% PWM → 0.5**2.2 * 254 ≈ 55
    assert body["brightness"] == 55
    assert "transition" not in body


async def test_set_color_temp_converts_k_to_mireds(make_light):
    lamp, client, _ = make_light()
    await lamp.do_command({"command": "set", "color_temp_k": 2500})
    _, body = client.published[-1]
    # 1_000_000 / 2500 = 400
    assert body["color_temp"] == 400


async def test_set_passes_transition_in_seconds(make_light):
    lamp, client, _ = make_light()
    await lamp.do_command(
        {"command": "set", "on": True, "brightness_pct": 80, "transition_s": 1800}
    )
    _, body = client.published[-1]
    assert body["transition"] == 1800
    assert body["state"] == "ON"


async def test_set_rejects_empty_command(make_light):
    lamp, _, _ = make_light()
    with pytest.raises(ValueError, match="at least one"):
        await lamp.do_command({"command": "set"})


async def test_set_rejects_out_of_range_kelvin(make_light):
    lamp, _, _ = make_light()
    with pytest.raises(ValueError, match="color_temp_k"):
        await lamp.do_command({"command": "set", "color_temp_k": 9000})


async def test_set_times_out_when_silent(make_light):
    lamp, _, _ = make_light(respond=False)
    with pytest.raises(TimeoutError, match="did not echo"):
        await lamp.do_command({"command": "set", "on": True})


async def test_set_fails_fast_when_disconnected(make_light):
    lamp, client, _ = make_light(connect=False)
    with pytest.raises(ConnectionError, match="not connected"):
        await lamp.do_command({"command": "set", "on": True})
    assert client.published == []


def test_brightness_mapping_is_nonlinear():
    assert light_mod._pct_to_zb_brightness(0) == 0
    assert light_mod._pct_to_zb_brightness(100) == 254
    # Perceived 50% is significantly LESS than linear 127 — that's the whole point.
    mid = light_mod._pct_to_zb_brightness(50)
    assert mid < 127
    # Monotonic non-decreasing.
    prev = -1
    for p in range(0, 101, 5):
        v = light_mod._pct_to_zb_brightness(p)
        assert v >= prev
        prev = v


async def test_connect_requests_state_and_sets_power_on_behavior(make_light):
    lamp, client, _ = make_light(connect=False, power_on_behavior="previous")
    client.connect()
    topics = [t for t, _ in client.published]
    assert f"{STATE_TOPIC}/get" in topics
    por_sets = [b for t, b in client.published if t == f"{STATE_TOPIC}/set"]
    assert any(b.get("power_on_behavior") == "previous" for b in por_sets)


async def test_availability_text_payload(make_light):
    lamp, client, _ = make_light()
    client.report(b"offline", topic=AVAIL_TOPIC)
    await settle()
    assert lamp._availability == "offline"
    client.report(b"online", topic=AVAIL_TOPIC)
    await settle()
    assert lamp._availability == "online"


async def test_availability_json_payload(make_light):
    lamp, client, _ = make_light()
    client.report({"state": "offline"}, topic=AVAIL_TOPIC)
    await settle()
    assert lamp._availability == "offline"


async def test_health_ok_after_successful_set(make_light):
    lamp, _, _ = make_light()
    await lamp.do_command({"command": "set", "on": True})
    status = await lamp.do_command({"command": "status"})
    assert status["health"] == "ok"
    assert status["message"] == ""
    assert status["kind"] == "light"


async def test_health_mqtt_down_when_disconnected(make_light):
    lamp, _, _ = make_light(connect=False)
    status = await lamp.do_command({"command": "status"})
    assert status["health"] == "mqtt_down"
    assert status["message"] == "broker offline"


async def test_health_lamp_power_off_from_availability(make_light):
    lamp, client, _ = make_light()
    client.report(b"offline", topic=AVAIL_TOPIC)
    await settle()
    status = await lamp.do_command({"command": "status"})
    assert status["health"] == "lamp_power_off"
    assert status["message"] == "switched off at the lamp"


async def test_health_zigbee_unreachable_after_timeout(make_light):
    lamp, _, _ = make_light(respond=False)
    with pytest.raises(TimeoutError):
        await lamp.do_command({"command": "set", "on": True})
    status = await lamp.do_command({"command": "status"})
    assert status["health"] == "zigbee_unreachable"
    assert status["message"] == "not responding"


def test_health_messages_cover_all_states():
    assert set(HEALTH_MESSAGES) == {
        "ok", "lamp_power_off", "zigbee_unreachable", "mqtt_down", "module_error",
    }


def _ramp(**overrides) -> dict:
    base = {
        "time": "06:30",
        "days_of_week": [0, 1, 2, 3, 4],
        "duration_s": 1800,
        "start_brightness_pct": 1.0,
        "start_color_temp_k": 2200,
        "target_brightness_pct": 80.0,
        "target_color_temp_k": 4000,
    }
    base.update(overrides)
    return base


async def test_add_schedule_normalizes_and_persists(make_light):
    lamp, _, state_path = make_light()
    resp = await lamp.do_command({"command": "add_schedule", "schedule": _ramp()})
    sched = resp["schedule"]
    assert sched["id"]
    assert sched["enabled"] is True
    assert sched["name"] == "Ramp 06:30"

    persisted = json.loads(Path(state_path).read_text())
    assert persisted["schema_version"] == 1
    assert len(persisted["schedules"]) == 1
    assert persisted["schedules"][0]["id"] == sched["id"]


async def test_modify_schedule(make_light):
    lamp, _, _ = make_light()
    add = await lamp.do_command({"command": "add_schedule", "schedule": _ramp()})
    sid = add["schedule"]["id"]
    resp = await lamp.do_command({
        "command": "modify_schedule",
        "schedule": {"id": sid, "target_brightness_pct": 100.0, "duration_s": 600},
    })
    assert resp["schedule"]["target_brightness_pct"] == 100.0
    assert resp["schedule"]["duration_s"] == 600
    assert resp["schedule"]["start_brightness_pct"] == 1.0


async def test_delete_schedule(make_light):
    lamp, _, _ = make_light()
    add = await lamp.do_command({"command": "add_schedule", "schedule": _ramp()})
    sid = add["schedule"]["id"]
    await lamp.do_command({"command": "delete_schedule", "id": sid})
    status = await lamp.do_command({"command": "status"})
    assert status["schedules"] == []


async def test_set_schedule_enabled(make_light):
    lamp, _, _ = make_light()
    add = await lamp.do_command({"command": "add_schedule", "schedule": _ramp()})
    sid = add["schedule"]["id"]
    await lamp.do_command({"command": "set_schedule_enabled", "id": sid, "enabled": False})
    status = await lamp.do_command({"command": "status"})
    assert status["schedules"][0]["enabled"] is False


async def test_reseed_from_config_when_state_absent(make_light, tmp_path):
    path = tmp_path / "fresh.json"
    assert not path.exists()
    lamp, _, _ = make_light(state_path=str(path), schedules=[_ramp()])
    status = await lamp.do_command({"command": "status"})
    assert len(status["schedules"]) == 1
    assert status["schedules"][0]["time"] == "06:30"


async def test_persisted_state_wins_over_config_seed(make_light, tmp_path):
    path = tmp_path / "persisted.json"
    path.write_text(json.dumps({
        "schema_version": 1,
        "schedules": [_ramp(time="07:00") | {"id": "abc12345", "enabled": True,
                                             "last_processed_at": None, "last_fired_at": None,
                                             "name": "Ramp 07:00"}],
    }))
    lamp, _, _ = make_light(state_path=str(path), schedules=[_ramp(time="06:00")])
    status = await lamp.do_command({"command": "status"})
    assert status["schedules"][0]["time"] == "07:00"


async def test_run_ramp_snaps_start_then_ramps_target(make_light):
    lamp, client, _ = make_light()
    await lamp._run_ramp(_ramp() | {"id": "x", "name": "n", "enabled": True,
                                     "last_processed_at": None, "last_fired_at": None})
    sets = [b for t, b in client.published if t == f"{STATE_TOPIC}/set"]
    assert len(sets) == 2
    assert sets[0]["transition"] == 0.0
    assert sets[0]["state"] == "ON"
    assert sets[0]["brightness"] == light_mod._pct_to_zb_brightness(1.0)
    assert sets[1]["transition"] == 1800.0
    assert sets[1]["brightness"] == light_mod._pct_to_zb_brightness(80.0)


async def test_unknown_command(make_light):
    lamp, _, _ = make_light()
    with pytest.raises(ValueError, match="Unknown command"):
        await lamp.do_command({"command": "banana"})


async def test_other_state_topics_ignored(make_light):
    lamp, client, _ = make_light()
    await settle()
    before = dict(lamp._last_state)
    client.report({"state": "OFF", "brightness": 1}, topic="zigbee2mqtt/some-other-lamp")
    await settle()
    assert lamp._last_state == before
