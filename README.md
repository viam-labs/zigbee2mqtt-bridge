# zigbee2mqtt-bridge

Viam module that subscribes to [zigbee2mqtt](https://www.zigbee2mqtt.io/) device topics and forwards every message carrying an `action` field into a Viam **events sensor** as a `push_event` do_command.

Primary use case: wire physical Zigbee buttons (ThirdReality, Aqara, Hue, Ikea, etc.) into a Viam machine's event stream so triggers, data capture, and downstream automations can react to them.

It also exposes on/off devices (smart plugs, relays) as Viam **switches** — see [switch](#switch).

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
| `actions` | no | `[]` | List of `{when, do}` dispatch rules — see below. |

### Dispatching do_commands on button presses

Beyond pushing to the events sensor, the bridge can call `do_command` on any other configured component when a specific event fires. Example — press `button-a` to decrement an inventory item:

```json
{
  "actions": [
    {
      "when": {"source": "button-a", "action": "single"},
      "do": {
        "component": "inventory",
        "command": {"command": "decrement", "id": "b06bcf436cff", "by": 1}
      }
    }
  ]
}
```

Each target `component` listed under `do` is automatically added as a required dependency, so viam-server resolves startup order correctly. Add the same component name under `depends_on` is not necessary.

`when` matches by `source` and/or `action`. Omitting a key matches any value — e.g. `{"source": "button-a"}` fires for every action on button-a.

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

## switch

Model: `viam:zigbee2mqtt:switch`, API: `rdk:component:switch`.

Controls one on/off zigbee2mqtt device, such as a smart plug. Position `0` is off and `1` is on.

`set_position` publishes `{"state": "OFF"|"ON"}` to `<topic_prefix>/<device>/set`, then waits until the device reports that state on `<topic_prefix>/<device>`. It raises if the broker is unreachable or the device doesn't confirm within `confirm_timeout_ms`, so callers know whether the device really switched.

### Config

```json
{
  "mqtt_server": "mqtt://localhost:1883",
  "device": "mixer-plug",
  "confirm_timeout_ms": 5000
}
```

| Attribute | Required | Default | Description |
|---|---|---|---|
| `mqtt_server` | yes | — | MQTT broker URL (`mqtt://host:port`). |
| `device` | yes | — | The device's zigbee2mqtt friendly name. |
| `topic_prefix` | no | `zigbee2mqtt` | zigbee2mqtt `base_topic`. |
| `confirm_timeout_ms` | no | `5000` | How long `set_position` (and `get_position` before the first report) waits for the device to report its state. |
| `mqtt_username` / `mqtt_password` | no | — | For brokers that require auth. |

On connect, the switch requests the device's current state (`<topic_prefix>/<device>/get`), and it tracks every state report afterward, so `get_position` reflects changes made outside Viam too.

Tested with the ThirdReality Smart Plug Gen3 (`3RSP02064Z`).

## Development

```bash
pip install -e '.[dev]'
pytest
```

`tests/test_switch_broker.py` runs end-to-end against a real broker with a simulated plug when `MQTT_TEST_SERVER` is set:

```bash
docker run --rm -d -p 127.0.0.1:1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
MQTT_TEST_SERVER=mqtt://localhost:1883 pytest
```
