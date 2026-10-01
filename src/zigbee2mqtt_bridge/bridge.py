"""zigbee2mqtt → events-sensor bridge.

Subscribes to zigbee2mqtt device topics over MQTT and forwards any
message that carries an `action` field as a push_event do_command to
a configured events sensor.
"""

import asyncio
import json
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, ClassVar, Self
from urllib.parse import urlparse

import paho.mqtt.client as mqtt
from viam.components.sensor import Sensor
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import ResourceName
from viam.resource.base import ResourceBase
from viam.resource.registry import Registry, ResourceCreatorRegistration
from viam.resource.types import Model, ModelFamily
from viam.utils import struct_to_dict

LOGGER = logging.getLogger(__name__)

DEFAULT_TOPIC_PREFIX = "zigbee2mqtt"


class Bridge(Sensor):
    MODEL: ClassVar[Model] = Model(ModelFamily("joseph", "zigbee2mqtt"), "bridge")

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._client: mqtt.Client | None = None
        self._events_sensor: Sensor | None = None
        self._events_sensor_name: str = ""
        self._topic_prefix: str = DEFAULT_TOPIC_PREFIX
        self._friendly_name_map: dict[str, str] = {}
        self._main_loop: asyncio.AbstractEventLoop | None = None
        self._message_count: int = 0
        self._last_event: dict | None = None
        self._connected: bool = False

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
        server = attrs.get("mqtt_server")
        if not isinstance(server, str) or not server:
            raise ValueError("`mqtt_server` is required (e.g. mqtt://localhost)")
        events = attrs.get("events_sensor")
        if not isinstance(events, str) or not events:
            raise ValueError("`events_sensor` is required")
        prefix = attrs.get("topic_prefix", DEFAULT_TOPIC_PREFIX)
        if not isinstance(prefix, str) or not prefix:
            raise ValueError("`topic_prefix` must be a non-empty string if set")
        name_map = attrs.get("friendly_name_map", {})
        if not isinstance(name_map, dict):
            raise ValueError("`friendly_name_map` must be an object if set")
        return [events], []

    def reconfigure(
        self,
        config: ComponentConfig,
        dependencies: Mapping[ResourceName, ResourceBase],
    ) -> None:
        attrs = struct_to_dict(config.attributes)
        self._topic_prefix = str(attrs.get("topic_prefix", DEFAULT_TOPIC_PREFIX)).rstrip("/")
        self._friendly_name_map = {
            str(k): str(v) for k, v in (attrs.get("friendly_name_map") or {}).items()
        }
        self._events_sensor_name = str(attrs["events_sensor"])
        self._events_sensor = None
        for name, resource in dependencies.items():
            if name.name == self._events_sensor_name and isinstance(resource, Sensor):
                self._events_sensor = resource
                break
        if self._events_sensor is None:
            LOGGER.warning(
                "events_sensor %r not found among dependencies; events will be dropped",
                self._events_sensor_name,
            )

        try:
            self._main_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._main_loop = None

        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass

        parsed = urlparse(str(attrs["mqtt_server"]))
        host = parsed.hostname or "localhost"
        port = parsed.port or 1883
        username = attrs.get("mqtt_username")
        password = attrs.get("mqtt_password")

        self._client = mqtt.Client()
        if isinstance(username, str) and username:
            self._client.username_pw_set(username, password if isinstance(password, str) else None)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        try:
            self._client.connect_async(host, port, keepalive=60)
            self._client.loop_start()
        except Exception as e:
            LOGGER.error("MQTT connect failed: %s", e)

    # -- MQTT callbacks (run on paho's background thread) --------------

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc == 0:
            self._connected = True
            topic = f"{self._topic_prefix}/+"
            client.subscribe(topic)
            LOGGER.info("MQTT connected; subscribed to %r", topic)
        else:
            self._connected = False
            LOGGER.error("MQTT connect failed with rc=%s", rc)

    def _on_disconnect(self, client, userdata, rc, properties=None, reason=None) -> None:
        self._connected = False
        LOGGER.warning("MQTT disconnected (rc=%s)", rc)

    def _on_message(self, client, userdata, msg) -> None:
        topic = msg.topic
        parts = topic.split("/")
        if len(parts) < 2:
            return
        # Skip zigbee2mqtt bridge/control topics.
        if parts[-1] == "bridge" or (len(parts) >= 2 and parts[-2] == "bridge"):
            return
        device = parts[-1]
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        action = payload.get("action")
        if not isinstance(action, str) or not action:
            return

        source = self._friendly_name_map.get(device, device)
        event: dict[str, Any] = {
            "event_type": "button_pressed",
            "source": source,
            "action": action,
            "topic": topic,
            "at": datetime.now(UTC).isoformat(),
        }
        if "battery" in payload:
            event["battery"] = payload["battery"]
        if "linkquality" in payload:
            event["linkquality"] = payload["linkquality"]

        self._last_event = event
        self._message_count += 1

        if self._events_sensor is None or self._main_loop is None:
            return
        asyncio.run_coroutine_threadsafe(
            self._push_event(event),
            self._main_loop,
        )

    async def _push_event(self, event: dict) -> None:
        assert self._events_sensor is not None
        try:
            await self._events_sensor.do_command({"command": "push_event", "event": event})
        except Exception as e:
            LOGGER.warning("push_event failed: %s", e)

    # -- Sensor API ----------------------------------------------------

    async def get_readings(
        self, *, extra: Mapping[str, Any] | None = None, timeout: float | None = None, **kwargs
    ) -> Mapping[str, Any]:
        return {
            "connected": self._connected,
            "message_count": self._message_count,
            "last_event": self._last_event or {},
            "topic_prefix": self._topic_prefix,
            "events_sensor": self._events_sensor_name,
        }

    async def close(self) -> None:
        if self._client is None:
            return
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass
        self._client = None


Registry.register_resource_creator(
    Sensor.API,
    Bridge.MODEL,
    ResourceCreatorRegistration(Bridge.new, Bridge.validate),
)
