"""Tunable-white Zigbee bulb (brightness + color temp) via zigbee2mqtt.

One Viam Generic component instance per bulb. Scheduled ramps fire in-lamp
via Zigbee's `transition` parameter — not polled from the module.
"""

import asyncio
import json
import logging
import re
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar, Self

import paho.mqtt.client as mqtt
from viam.components.generic import Generic
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import ResourceName
from viam.resource.base import ResourceBase
from viam.resource.registry import Registry, ResourceCreatorRegistration
from viam.resource.types import Model, ModelFamily
from viam.utils import struct_to_dict

from .mqtt_client import start_client, stop_client, topic_prefix, validate_connection_attrs

LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = 1

DEFAULT_STATE_PATH = "~/.viam/zigbee2mqtt-light-{name}-state.json"
DEFAULT_CONFIRM_TIMEOUT_MS = 5000

BG_LOOP_INTERVAL_SEC = 60
CATCH_UP_WINDOW_MIN = 30

MIN_K = 2200
MAX_K = 6500

ZB_BRIGHTNESS_MAX = 254
# Human perception of brightness roughly follows a gamma curve, so a linear
# ramp from 1 to 254 feels like it shoots up and then creeps. Apply gamma
# at the module boundary; the user sees percent, the lamp sees PWM.
GAMMA = 2.2

POWER_ON_CHOICES = ("on", "off", "toggle", "previous")
DEFAULT_POWER_ON_BEHAVIOR = "previous"

HEALTH_MESSAGES: dict[str, str] = {
    "ok": "",
    "lamp_power_off": "switched off at the lamp",
    "zigbee_unreachable": "not responding",
    "mqtt_down": "broker offline",
    "module_error": "error",
}

_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


def _new_id() -> str:
    return uuid.uuid4().hex[:8]


def _parse_hhmm(value: Any) -> tuple[int, int]:
    if not isinstance(value, str):
        raise ValueError("`time` must be HH:MM")
    m = _TIME_RE.match(value)
    if not m:
        raise ValueError("`time` must be HH:MM")
    h, mi = int(m.group(1)), int(m.group(2))
    if not (0 <= h < 24 and 0 <= mi < 60):
        raise ValueError("`time` out of range")
    return h, mi


def _normalize_days(days: Any) -> list[int]:
    """0..6 = Mon..Sun. Empty list = every day.

    Protobuf serializes numeric fields as doubles, so [0,1,2] arrives as
    [0.0, 1.0, 2.0]. Coerce int-valued floats.
    """
    if days is None:
        return []
    if not isinstance(days, list):
        raise ValueError("`days_of_week` must be a list of integers 0..6")
    out: set[int] = set()
    for d in days:
        if isinstance(d, bool):
            raise ValueError("`days_of_week` values must be integers 0..6")
        if isinstance(d, int):
            di = d
        elif isinstance(d, float) and d.is_integer():
            di = int(d)
        else:
            raise ValueError("`days_of_week` values must be integers 0..6")
        if not 0 <= di <= 6:
            raise ValueError("`days_of_week` values must be integers 0..6")
        out.add(di)
    return sorted(out)


def _check_pct(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"`{name}` must be a number 0..100")
    f = float(value)
    if not 0.0 <= f <= 100.0:
        raise ValueError(f"`{name}` must be 0..100")
    return f


def _check_kelvin(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"`{name}` must be a number in Kelvin")
    k = int(value)
    if not MIN_K <= k <= MAX_K:
        raise ValueError(f"`{name}` must be {MIN_K}..{MAX_K}")
    return k


def _check_duration(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError("`duration_s` must be a positive number")
    return int(value)


def _normalize_schedule(raw: dict) -> dict:
    if not isinstance(raw, dict):
        raise ValueError("schedule must be an object")
    h, mi = _parse_hhmm(raw.get("time"))
    return {
        "id": raw.get("id") or _new_id(),
        "name": str(raw.get("name") or f"Ramp {h:02d}:{mi:02d}"),
        "time": f"{h:02d}:{mi:02d}",
        "days_of_week": _normalize_days(raw.get("days_of_week")),
        "duration_s": _check_duration(raw.get("duration_s")),
        "start_brightness_pct": _check_pct("start_brightness_pct", raw.get("start_brightness_pct")),
        "start_color_temp_k": _check_kelvin("start_color_temp_k", raw.get("start_color_temp_k")),
        "target_brightness_pct": _check_pct(
            "target_brightness_pct", raw.get("target_brightness_pct")
        ),
        "target_color_temp_k": _check_kelvin("target_color_temp_k", raw.get("target_color_temp_k")),
        "enabled": bool(raw.get("enabled", True)),
        "last_processed_at": raw.get("last_processed_at"),
        "last_fired_at": raw.get("last_fired_at"),
    }


def _empty_state() -> dict:
    return {"schema_version": SCHEMA_VERSION, "schedules": []}


def _k_to_mireds(k: int) -> int:
    return round(1_000_000 / k)


def _mireds_to_k(m: int | float) -> int:
    if m <= 0:
        return MAX_K
    return round(1_000_000 / m)


def _pct_to_zb_brightness(pct: float) -> int:
    if pct <= 0:
        return 0
    zb = round((pct / 100.0) ** GAMMA * ZB_BRIGHTNESS_MAX)
    return max(1, min(ZB_BRIGHTNESS_MAX, zb))


def _zb_brightness_to_pct(zb: int | float) -> float:
    if zb <= 0:
        return 0.0
    return round((zb / ZB_BRIGHTNESS_MAX) ** (1.0 / GAMMA) * 100.0, 1)


class Light(Generic):
    MODEL: ClassVar[Model] = Model(ModelFamily("viam", "zigbee2mqtt"), "light")

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._client: mqtt.Client | None = None
        self._main_loop: asyncio.AbstractEventLoop | None = None
        self._state_topic: str = ""
        self._avail_topic: str = ""
        self._confirm_timeout_s: float = DEFAULT_CONFIRM_TIMEOUT_MS / 1000
        self._power_on_behavior: str = DEFAULT_POWER_ON_BEHAVIOR
        self._state_path: str = ""

        self._connected: bool = False
        self._last_state: dict[str, Any] = {}
        self._last_state_at: str | None = None
        self._availability: str | None = None
        self._last_cmd_result: str | None = None

        self._waiters: list[asyncio.Future[None]] = []
        self._state_lock: asyncio.Lock | None = None
        self._state: dict | None = None
        self._bg_task: asyncio.Task | None = None

    @classmethod
    def new(
        cls,
        config: ComponentConfig,
        dependencies: Mapping[ResourceName, ResourceBase],
    ) -> Self:
        instance = cls(config.name)
        instance.reconfigure(config, dependencies)
        return instance

    @classmethod
    def validate(cls, config: ComponentConfig) -> tuple[Sequence[str], Sequence[str]]:
        attrs = struct_to_dict(config.attributes)
        validate_connection_attrs(attrs)
        device = attrs.get("device")
        if not isinstance(device, str) or not device or "/" in device:
            raise ValueError("`device` is required: zigbee2mqtt friendly name, no slashes")
        timeout = attrs.get("confirm_timeout_ms", DEFAULT_CONFIRM_TIMEOUT_MS)
        if isinstance(timeout, bool) or not isinstance(timeout, int | float) or timeout <= 0:
            raise ValueError("`confirm_timeout_ms` must be > 0 if set")
        state_path = attrs.get("state_path")
        if state_path is not None and not isinstance(state_path, str):
            raise ValueError("`state_path` must be a string if set")
        pob = attrs.get("power_on_behavior", DEFAULT_POWER_ON_BEHAVIOR)
        if pob is not None and pob not in POWER_ON_CHOICES:
            raise ValueError(f"`power_on_behavior` must be one of {POWER_ON_CHOICES}")
        for entry in attrs.get("schedules") or []:
            _normalize_schedule(entry)
        return [], []

    def reconfigure(
        self,
        config: ComponentConfig,
        dependencies: Mapping[ResourceName, ResourceBase],
    ) -> None:
        attrs = struct_to_dict(config.attributes)
        self._state_topic = f"{topic_prefix(attrs)}/{attrs['device']}"
        self._avail_topic = f"{self._state_topic}/availability"
        self._confirm_timeout_s = (
            float(attrs.get("confirm_timeout_ms", DEFAULT_CONFIRM_TIMEOUT_MS)) / 1000
        )
        self._power_on_behavior = str(attrs.get("power_on_behavior") or DEFAULT_POWER_ON_BEHAVIOR)
        self._state_path = str(
            attrs.get("state_path") or DEFAULT_STATE_PATH.format(name=config.name)
        )
        self._main_loop = asyncio.get_running_loop()

        stop_client(self._client)
        self._connected = False
        self._last_state = {}
        self._last_state_at = None
        self._availability = None
        self._last_cmd_result = None

        self._state_lock = asyncio.Lock()
        self._state = self._load_and_seed_state(attrs)

        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
        try:
            self._bg_task = asyncio.create_task(self._bg_loop())
        except RuntimeError:
            self._bg_task = None

        self._client = start_client(
            attrs,
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
            on_message=self._on_message,
        )

    def _load_and_seed_state(self, attrs: dict) -> dict:
        loaded = self._load_state()
        state = _empty_state()
        if isinstance(loaded.get("schedules"), list):
            state["schedules"] = loaded["schedules"]
        if not state["schedules"]:
            seed_raw = attrs.get("schedules") or []
            for entry in seed_raw:
                try:
                    state["schedules"].append(_normalize_schedule(entry))
                except ValueError as e:
                    LOGGER.warning("dropping malformed seed schedule: %s", e)
        return state

    def _load_state(self) -> dict:
        path = Path(self._state_path).expanduser()
        if not path.exists():
            return _empty_state()
        try:
            loaded = json.loads(path.read_text())
        except Exception as e:
            LOGGER.warning("failed to load state from %s: %s", path, e)
            return _empty_state()
        if not isinstance(loaded, dict):
            return _empty_state()
        merged = _empty_state()
        if isinstance(loaded.get("schedules"), list):
            valid = []
            for entry in loaded["schedules"]:
                try:
                    valid.append(_normalize_schedule(entry))
                except ValueError as e:
                    LOGGER.warning("dropping malformed persisted schedule: %s", e)
            merged["schedules"] = valid
        if loaded.get("schema_version") == SCHEMA_VERSION:
            merged["schema_version"] = SCHEMA_VERSION
        return merged

    def _save_state(self) -> None:
        """Atomic write. Caller must hold _state_lock."""
        assert self._state is not None
        path = Path(self._state_path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._state, indent=2))
        tmp.replace(path)

    def _find_schedule(self, sid: str) -> dict | None:
        for s in (self._state or {}).get("schedules", []):
            if s.get("id") == sid:
                return s
        return None

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            self._connected = False
            LOGGER.error("MQTT connect failed: %s", reason_code)
            return
        self._connected = True
        client.subscribe(self._state_topic, qos=1)
        client.subscribe(self._avail_topic, qos=1)
        client.publish(
            f"{self._state_topic}/get",
            json.dumps({"state": "", "brightness": "", "color_temp": ""}),
            qos=1,
        )
        client.publish(
            f"{self._state_topic}/set",
            json.dumps({"power_on_behavior": self._power_on_behavior}),
            qos=1,
        )
        LOGGER.info("MQTT connected; subscribed to %r", self._state_topic)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties) -> None:
        self._connected = False
        LOGGER.warning("MQTT disconnected: %s", reason_code)

    def _on_message(self, client, userdata, msg) -> None:
        if self._main_loop is None:
            return
        if msg.topic == self._avail_topic:
            # z2m v2 publishes "online"/"offline" as plain text, but older
            # configs wrap it as {"state": "online"}. Handle both.
            raw = msg.payload.decode("utf-8", errors="replace").strip()
            avail: str | None = None
            if raw in ("online", "offline"):
                avail = raw
            else:
                try:
                    body = json.loads(raw)
                    if isinstance(body, dict) and body.get("state") in ("online", "offline"):
                        avail = body["state"]
                except Exception:
                    pass
            if avail is not None:
                self._main_loop.call_soon_threadsafe(self._record_availability, avail)
            return
        if msg.topic != self._state_topic:
            return
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        self._main_loop.call_soon_threadsafe(self._record_state_echo, payload)

    def _record_state_echo(self, payload: dict) -> None:
        self._last_state = payload
        self._last_state_at = datetime.now(UTC).isoformat()
        remaining: list[asyncio.Future[None]] = []
        for fut in self._waiters:
            if fut.done():
                continue
            fut.set_result(None)
        self._waiters = remaining

    def _record_availability(self, avail: str) -> None:
        self._availability = avail

    async def _publish_and_wait(self, topic: str, payload: dict) -> None:
        if self._client is None or not self._connected:
            self._last_cmd_result = "mqtt_disconnected"
            raise ConnectionError(f"not connected to the MQTT broker; cannot reach {topic!r}")
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        try:
            info = self._client.publish(topic, json.dumps(payload), qos=1)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                self._last_cmd_result = "publish_failed"
                raise ConnectionError(
                    f"MQTT publish to {topic!r} failed: {mqtt.error_string(info.rc)}"
                )
            await asyncio.wait_for(fut, self._confirm_timeout_s)
            self._last_cmd_result = "ok"
        except TimeoutError:
            self._last_cmd_result = "timed_out"
            raise TimeoutError(
                f"{self._state_topic!r} did not echo within {self._confirm_timeout_s:g}s; "
                "is zigbee2mqtt running and the bulb reachable?"
            ) from None
        finally:
            fut.cancel()
            self._waiters = [f for f in self._waiters if f is not fut]

    async def _set(
        self,
        *,
        on: bool | None = None,
        brightness_pct: float | None = None,
        color_temp_k: int | None = None,
        transition_s: float | None = None,
    ) -> dict:
        payload: dict[str, Any] = {}
        if on is not None:
            payload["state"] = "ON" if on else "OFF"
        if brightness_pct is not None:
            pct = _check_pct("brightness_pct", brightness_pct)
            payload["brightness"] = _pct_to_zb_brightness(pct)
        if color_temp_k is not None:
            payload["color_temp"] = _k_to_mireds(_check_kelvin("color_temp_k", color_temp_k))
        if transition_s is not None:
            if isinstance(transition_s, bool) or not isinstance(transition_s, int | float):
                raise ValueError("`transition_s` must be a number")
            if transition_s < 0:
                raise ValueError("`transition_s` must be >= 0")
            payload["transition"] = float(transition_s)
        if not any(k in payload for k in ("state", "brightness", "color_temp")):
            raise ValueError("`set` needs at least one of on, brightness_pct, color_temp_k")
        await self._publish_and_wait(f"{self._state_topic}/set", payload)
        return {"ok": True}

    async def _get(self) -> dict:
        await self._publish_and_wait(
            f"{self._state_topic}/get",
            {"state": "", "brightness": "", "color_temp": ""},
        )
        return self._readings()

    def _readings(self) -> dict:
        out: dict[str, Any] = {"state": self._last_state.get("state")}
        b = self._last_state.get("brightness")
        if isinstance(b, int | float):
            out["brightness_pct"] = _zb_brightness_to_pct(b)
        m = self._last_state.get("color_temp")
        if isinstance(m, int | float):
            out["color_temp_k"] = _mireds_to_k(m)
        return out

    def _health(self) -> str:
        if not self._connected:
            return "mqtt_down"
        if self._availability == "offline":
            return "lamp_power_off"
        if self._last_cmd_result == "timed_out":
            return "zigbee_unreachable"
        return "ok"

    async def _status(self) -> dict:
        health = self._health()
        return {
            "kind": "light",
            "health": health,
            "message": HEALTH_MESSAGES[health],
            "mqtt_connected": self._connected,
            "device_available": self._availability,
            "last_state_at": self._last_state_at,
            "last_cmd_result": self._last_cmd_result,
            "readings": self._readings(),
            "schedules": list((self._state or {}).get("schedules", [])),
        }

    async def _run_ramp(self, schedule: dict) -> None:
        """Snap to the start state, then ramp to the target.

        Zigbee's `transition` does nothing when the lamp is OFF or already at
        the target brightness, so we force the start explicitly with a
        zero-second transition before starting the real ramp. That also makes
        a sunrise deterministic regardless of where the bulb was left.
        """
        await self._set(
            on=True,
            brightness_pct=schedule["start_brightness_pct"],
            color_temp_k=schedule["start_color_temp_k"],
            transition_s=0.0,
        )
        await self._set(
            on=True,
            brightness_pct=schedule["target_brightness_pct"],
            color_temp_k=schedule["target_color_temp_k"],
            transition_s=float(schedule["duration_s"]),
        )

    async def _bg_loop(self) -> None:
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                LOGGER.warning("bg loop tick failed: %s", e)
            await asyncio.sleep(BG_LOOP_INTERVAL_SEC)

    async def _tick(self) -> None:
        assert self._state is not None
        assert self._state_lock is not None
        now_local = datetime.now().astimezone()
        state_changed = False
        async with self._state_lock:
            for schedule in list(self._state.get("schedules", [])):
                try:
                    if await self._process_schedule(schedule, now_local):
                        state_changed = True
                except Exception as e:
                    LOGGER.warning("failed to process schedule %s: %s", schedule.get("id"), e)
            if state_changed:
                self._save_state()

    async def _process_schedule(self, schedule: dict, now_local: datetime) -> bool:
        if not schedule.get("enabled", True):
            return False
        try:
            h, mi = _parse_hhmm(schedule.get("time"))
        except ValueError:
            return False
        today_fire = now_local.replace(hour=h, minute=mi, second=0, microsecond=0)
        if now_local < today_fire:
            return False
        dows = schedule.get("days_of_week") or []
        if dows and now_local.weekday() not in dows:
            return False

        last_proc_iso = schedule.get("last_processed_at")
        if last_proc_iso:
            try:
                last_proc = datetime.fromisoformat(last_proc_iso).astimezone(now_local.tzinfo)
                if last_proc >= today_fire:
                    return False
            except ValueError:
                pass

        age_min = (now_local - today_fire).total_seconds() / 60
        if age_min > CATCH_UP_WINDOW_MIN:
            LOGGER.warning(
                "missed ramp for schedule %s at %s (%.0f min past catch-up window)",
                schedule.get("id"),
                today_fire.isoformat(),
                age_min - CATCH_UP_WINDOW_MIN,
            )
            schedule["last_processed_at"] = today_fire.astimezone(UTC).isoformat()
            return True

        fired_at_iso = datetime.now(UTC).isoformat()
        try:
            await self._run_ramp(schedule)
            schedule["last_fired_at"] = fired_at_iso
        except Exception as e:
            LOGGER.warning("ramp failed for schedule %s: %s", schedule.get("id"), e)
        schedule["last_processed_at"] = fired_at_iso
        return True

    async def _add_schedule(self, payload: Any) -> dict:
        assert self._state is not None
        assert self._state_lock is not None
        schedule = _normalize_schedule({**(payload or {}), "id": _new_id()})
        async with self._state_lock:
            self._state["schedules"].append(schedule)
            self._save_state()
        return {"ok": True, "schedule": schedule}

    async def _modify_schedule(self, payload: Any) -> dict:
        assert self._state is not None
        assert self._state_lock is not None
        if not isinstance(payload, dict) or not payload.get("id"):
            raise ValueError("`id` is required")
        async with self._state_lock:
            existing = self._find_schedule(str(payload["id"]))
            if existing is None:
                raise ValueError(f"no schedule with id={payload['id']!r}")
            merged = {**existing}
            for k, v in payload.items():
                if v is not None or k == "days_of_week":
                    merged[k] = v
            normalized = _normalize_schedule(merged)
            normalized["id"] = existing["id"]
            for k in ("last_processed_at", "last_fired_at"):
                if k not in payload:
                    normalized[k] = existing.get(k)
            for i, s in enumerate(self._state["schedules"]):
                if s["id"] == existing["id"]:
                    self._state["schedules"][i] = normalized
                    break
            self._save_state()
        return {"ok": True, "schedule": normalized}

    async def _delete_schedule(self, payload: Any) -> dict:
        assert self._state is not None
        assert self._state_lock is not None
        sid = payload if isinstance(payload, str) else (payload or {}).get("id")
        if not isinstance(sid, str) or not sid:
            raise ValueError("`id` is required")
        async with self._state_lock:
            before = len(self._state["schedules"])
            self._state["schedules"] = [s for s in self._state["schedules"] if s.get("id") != sid]
            if len(self._state["schedules"]) == before:
                raise ValueError(f"no schedule with id={sid!r}")
            self._save_state()
        return {"ok": True, "id": sid}

    async def _set_schedule_enabled(self, payload: Any) -> dict:
        assert self._state is not None
        assert self._state_lock is not None
        if not isinstance(payload, dict) or not payload.get("id"):
            raise ValueError("`id` is required")
        sid = str(payload["id"])
        enabled = bool(payload.get("enabled"))
        async with self._state_lock:
            live = self._find_schedule(sid)
            if live is None:
                raise ValueError(f"no schedule with id={sid!r}")
            live["enabled"] = enabled
            self._save_state()
        return {"ok": True, "id": sid, "enabled": enabled}

    async def do_command(
        self,
        command: Mapping[str, Any],
        *,
        timeout: float | None = None,
        **kwargs,
    ) -> Mapping[str, Any]:
        cmd = command.get("command")
        if cmd == "status":
            return await self._status()
        if cmd == "get":
            return await self._get()
        if cmd == "set":
            return await self._set(
                on=command.get("on"),
                brightness_pct=command.get("brightness_pct"),
                color_temp_k=command.get("color_temp_k"),
                transition_s=command.get("transition_s"),
            )
        if cmd == "add_schedule":
            return await self._add_schedule(command.get("schedule") or command)
        if cmd == "modify_schedule":
            return await self._modify_schedule(command.get("schedule") or command)
        if cmd == "delete_schedule":
            return await self._delete_schedule(command)
        if cmd == "set_schedule_enabled":
            return await self._set_schedule_enabled(command)
        raise ValueError(f"Unknown command: {cmd!r}")

    async def close(self) -> None:
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
        stop_client(self._client)
        self._client = None
        for fut in self._waiters:
            fut.cancel()
        self._waiters = []


Registry.register_resource_creator(
    Generic.API,
    Light.MODEL,
    ResourceCreatorRegistration(Light.new, Light.validate),
)
