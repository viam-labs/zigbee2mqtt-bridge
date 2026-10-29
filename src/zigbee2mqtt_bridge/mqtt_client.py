"""MQTT connection setup shared by the bridge and switch models."""

from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlparse

import paho.mqtt.client as mqtt
from viam.logging import getLogger

LOGGER = getLogger(__name__)

DEFAULT_TOPIC_PREFIX = "zigbee2mqtt"


def validate_connection_attrs(attrs: Mapping[str, Any]) -> None:
    server = attrs.get("mqtt_server")
    if not isinstance(server, str) or not server:
        raise ValueError("`mqtt_server` is required (e.g. mqtt://localhost)")
    prefix = attrs.get("topic_prefix", DEFAULT_TOPIC_PREFIX)
    if not isinstance(prefix, str) or not prefix:
        raise ValueError("`topic_prefix` must be a non-empty string if set")


def topic_prefix(attrs: Mapping[str, Any]) -> str:
    return str(attrs.get("topic_prefix", DEFAULT_TOPIC_PREFIX)).rstrip("/")


def start_client(
    attrs: Mapping[str, Any],
    *,
    on_connect: Callable[..., None],
    on_disconnect: Callable[..., None],
    on_message: Callable[..., None],
) -> mqtt.Client:
    """Create a client and start connecting in the background (paho reconnects on its own)."""
    parsed = urlparse(str(attrs["mqtt_server"]))
    host = parsed.hostname or "localhost"
    port = parsed.port or 1883
    username = attrs.get("mqtt_username")
    password = attrs.get("mqtt_password")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if isinstance(username, str) and username:
        client.username_pw_set(username, password if isinstance(password, str) else None)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    try:
        client.connect_async(host, port, keepalive=60)
        client.loop_start()
    except Exception as e:
        LOGGER.error("MQTT connect failed: %s", e)
    return client


def stop_client(client: mqtt.Client | None) -> None:
    if client is None:
        return
    try:
        client.loop_stop()
        client.disconnect()
    except Exception:
        pass
