"""On/off zigbee2mqtt device (smart plug, relay) as a Viam switch.

Position 0 publishes {"state": "OFF"} and position 1 {"state": "ON"} to
`<prefix>/<device>/set`, then waits for the device to report that state on
`<prefix>/<device>` so callers know the command actually took effect.
"""

import asyncio
import json
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, Self

import paho.mqtt.client as mqtt
from viam.components.switch import Switch
from viam.logging import getLogger
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import ResourceName
from viam.resource.base import ResourceBase
from viam.resource.registry import Registry, ResourceCreatorRegistration
from viam.resource.types import Model, ModelFamily
from viam.utils import struct_to_dict

from .mqtt_client import start_client, stop_client, topic_prefix, validate_connection_attrs

LOGGER = getLogger(__name__)

DEFAULT_CONFIRM_TIMEOUT_MS = 5000
STATES = ("OFF", "ON")


class OnOffSwitch(Switch):
    MODEL: ClassVar[Model] = Model(ModelFamily("viam", "zigbee2mqtt"), "switch")

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._client: mqtt.Client | None = None
        self._main_loop: asyncio.AbstractEventLoop | None = None
        self._state_topic: str = ""
        self._confirm_timeout_s: float = DEFAULT_CONFIRM_TIMEOUT_MS / 1000
        self._connected: bool = False
        self._state: str | None = None
        self._waiters: list[tuple[str | None, asyncio.Future[None]]] = []

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
            raise ValueError("`device` is required: the zigbee2mqtt friendly name, without slashes")
        timeout = attrs.get("confirm_timeout_ms", DEFAULT_CONFIRM_TIMEOUT_MS)
        if not isinstance(timeout, int | float) or timeout <= 0:
            raise ValueError("`confirm_timeout_ms` must be > 0 if set")
        return [], []

    def reconfigure(
        self,
        config: ComponentConfig,
        dependencies: Mapping[ResourceName, ResourceBase],
    ) -> None:
        attrs = struct_to_dict(config.attributes)
        self._state_topic = f"{topic_prefix(attrs)}/{attrs['device']}"
        self._confirm_timeout_s = (
            float(attrs.get("confirm_timeout_ms", DEFAULT_CONFIRM_TIMEOUT_MS)) / 1000
        )
        self._main_loop = asyncio.get_running_loop()

        stop_client(self._client)
        self._connected = False
        self._state = None
        self._client = start_client(
            attrs,
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
            on_message=self._on_message,
        )

    # -- MQTT callbacks (run on paho's background thread) --------------

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            self._connected = False
            LOGGER.error("MQTT connect failed: %s", reason_code)
            return
        self._connected = True
        client.subscribe(self._state_topic, qos=1)
        client.publish(f"{self._state_topic}/get", json.dumps({"state": ""}), qos=1)
        LOGGER.info("MQTT connected; subscribed to %r", self._state_topic)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties) -> None:
        self._connected = False
        LOGGER.warning("MQTT disconnected: %s", reason_code)

    def _on_message(self, client, userdata, msg) -> None:
        if msg.topic != self._state_topic:
            return
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        state = payload.get("state")
        if state not in STATES or self._main_loop is None:
            return
        self._main_loop.call_soon_threadsafe(self._record_state, state)

    # -- state tracking (event loop only) --------------------------------

    def _record_state(self, state: str) -> None:
        self._state = state
        remaining = []
        for want, fut in self._waiters:
            if fut.done():
                continue
            if want is None or want == state:
                fut.set_result(None)
            else:
                remaining.append((want, fut))
        self._waiters = remaining

    async def _publish_and_wait(self, topic: str, payload: dict, want: str | None) -> None:
        """Publish, then wait until the device reports `want` (or any state if None)."""
        if self._client is None or not self._connected:
            raise ConnectionError(f"not connected to the MQTT broker; cannot reach {topic!r}")
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append((want, fut))
        try:
            info = self._client.publish(topic, json.dumps(payload), qos=1)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise ConnectionError(
                    f"MQTT publish to {topic!r} failed: {mqtt.error_string(info.rc)}"
                )
            await asyncio.wait_for(fut, self._confirm_timeout_s)
        except TimeoutError:
            raise TimeoutError(
                f"{self._state_topic!r} did not report state {want or 'any'} within "
                f"{self._confirm_timeout_s:g}s; is zigbee2mqtt running and the device reachable?"
            ) from None
        finally:
            fut.cancel()
            self._waiters = [(w, f) for w, f in self._waiters if f is not fut]

    # -- Switch API ----------------------------------------------------

    async def set_position(
        self,
        position: int,
        *,
        extra: Mapping[str, Any] | None = None,
        timeout: float | None = None,
        **kwargs,
    ) -> None:
        if position not in (0, 1):
            raise ValueError(f"position must be 0 (off) or 1 (on), got {position}")
        want = STATES[position]
        await self._publish_and_wait(f"{self._state_topic}/set", {"state": want}, want)

    async def get_position(
        self,
        *,
        extra: Mapping[str, Any] | None = None,
        timeout: float | None = None,
        **kwargs,
    ) -> int:
        if self._state is None:
            await self._publish_and_wait(f"{self._state_topic}/get", {"state": ""}, None)
        return STATES.index(self._state)

    async def get_number_of_positions(
        self,
        *,
        extra: Mapping[str, Any] | None = None,
        timeout: float | None = None,
        **kwargs,
    ) -> tuple[int, Sequence[str]]:
        return 2, ["off", "on"]

    async def close(self) -> None:
        stop_client(self._client)
        self._client = None
        for _, fut in self._waiters:
            fut.cancel()
        self._waiters = []


Registry.register_resource_creator(
    Switch.API,
    OnOffSwitch.MODEL,
    ResourceCreatorRegistration(OnOffSwitch.new, OnOffSwitch.validate),
)
