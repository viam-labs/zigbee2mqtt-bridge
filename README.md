# zigbee2mqtt-bridge

Viam module that subscribes to [zigbee2mqtt](https://www.zigbee2mqtt.io/) device topics and forwards every message carrying an `action` field into a Viam **events sensor** as a `push_event` do_command.

Primary use case: wire physical Zigbee buttons (ThirdReality, Aqara, Hue, Ikea, etc.) into a Viam machine's event stream so triggers, data capture, and downstream automations can react to them.

## bridge

Model: `joseph:zigbee2mqtt:bridge`, API: `rdk:component:sensor`.

### Config

```json
{
  "mqtt_server": "mqtt://localhost",
  "events_sensor": "events",
  "topic_prefix": "zigbee2mqtt",
  "friendly_name_map": {
    "0xffffb40e060984b1": "kitchen-button",
    "0xffffb40e060981ad": "bedroom-button"
  },
  "mqtt_username": null,
  "mqtt_password": null
}
```

| Attribute | Required | Default | Description |
|---|---|---|---|
| `mqtt_server` | yes | — | MQTT broker URL (`mqtt://host:port`). |
| `events_sensor` | yes | — | Name of a sensor dependency that accepts `do_command({"command": "push_event", "event": {...}})`. The [`viam:event-queue:sensor`](https://github.com/viam-labs/event-queue) module fits. |
| `topic_prefix` | no | `zigbee2mqtt` | Topic namespace. Subscribes to `<prefix>/+`. |
| `friendly_name_map` | no | `{}` | IEEE → nicer source label (set `source` on pushed events). Falls back to the raw topic segment. |
| `mqtt_username` / `mqtt_password` | no | — | For brokers that require auth. |

### Event shape

For every incoming message with a non-empty string `action` field, pushes:

```json
{
  "event_type": "button_pressed",
  "source": "kitchen-button",
  "action": "single",
  "topic": "zigbee2mqtt/0xffffb40e060984b1",
  "at": "2026-10-01T15:12:04.123+00:00",
  "battery": 100,
  "linkquality": 184
}
```

Messages without an `action` field are ignored. Messages on `zigbee2mqtt/bridge/*` (device announcements, config, etc.) are ignored.

### Readings

`get_readings` surfaces:
- `connected` — current MQTT connection state
- `message_count` — total actions forwarded since start
- `last_event` — the most recent event pushed
- `topic_prefix` / `events_sensor` — current config echo
