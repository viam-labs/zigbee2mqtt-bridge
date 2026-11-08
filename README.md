# zigbee2mqtt-bridge

Viam module that subscribes to [zigbee2mqtt](https://www.zigbee2mqtt.io/) device topics and forwards every message carrying an `action` field into a Viam **events sensor** as a `push_event` do_command.

Primary use case: wire physical Zigbee buttons (ThirdReality, Aqara, Hue, Ikea, etc.) into a Viam machine's event stream so triggers, data capture, and downstream automations can react to them.

It also exposes on/off devices (smart plugs, relays) as Viam **switches** — see [switch](#switch) — and tunable-white bulbs with scheduled in-lamp ramps as Viam **generics** — see [light](#light).

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

## light

Model: `viam:zigbee2mqtt:light`, API: `rdk:component:generic`.

One instance per Zigbee tunable-white bulb. Exposes brightness (0–100 %), color temperature (2200–6500 K), and on/off via `do_command`, and runs scheduled ramps that use the lamp's own Zigbee `transition` — the bulb interpolates, the module doesn't poll-and-step.

### Config

```json
{
  "mqtt_server": "mqtt://localhost",
  "device": "bedroom-lamp",
  "confirm_timeout_ms": 5000,
  "power_on_behavior": "previous",
  "schedules": []
}
```

| Attribute | Required | Default | Description |
|---|---|---|---|
| `mqtt_server` | yes | — | MQTT broker URL. |
| `device` | yes | — | The bulb's zigbee2mqtt friendly name. |
| `topic_prefix` | no | `zigbee2mqtt` | zigbee2mqtt `base_topic`. |
| `confirm_timeout_ms` | no | `5000` | How long each `set`/`get` waits for the bulb's state echo. |
| `power_on_behavior` | no | `previous` | Behavior on mains restoration (`previous`, `off`, `on`, `toggle`). Written to the bulb on every MQTT reconnect. See [power-on behavior](#power-on-behavior). |
| `state_path` | no | `~/.viam/zigbee2mqtt-light-<name>-state.json` | Where schedules persist. |
| `schedules` | no | `[]` | Initial schedules; applied only if no state file exists yet. |
| `mqtt_username` / `mqtt_password` | no | — | For brokers that require auth. |

### Perceptual brightness

Zigbee carries brightness as a linear 0–254 PWM value, but human perception follows a gamma curve — 127/254 feels much brighter than half. The module gamma-maps the `brightness_pct` the caller sends (0–100) before publishing to MQTT, so a sunrise from 1 % to 80 % feels smooth instead of jumping in the first few seconds and then crawling.

### Power-on behavior

Hue firmware defaults vary across bulb revisions: some restore the last state, some come back at full brightness and 2700 K every time. If you have the bulb on a physical foot switch and someone flips it at 11 pm, "full brightness / cool white" every time is worse than no bulb at all.

The module writes the `power_on_behavior` config attribute to the bulb (`<prefix>/<device>/set` with `{"power_on_behavior": "..."}`) on every MQTT reconnect. Choices: `previous` (restore the last brightness/CT/state), `off`, `on`, or `toggle`. Default: `previous`.

### Scheduled ramps

Each row runs once per day at `HH:MM`, snapping the bulb to the start state (with a zero-second transition — so the bulb is in a known state even if it was off), then handing the ramp to the bulb's Zigbee firmware with `duration_s` in the payload.

```json
{
  "schedules": [
    {
      "time": "06:30",
      "days_of_week": [0, 1, 2, 3, 4],
      "duration_s": 1800,
      "start_brightness_pct": 1.0,
      "start_color_temp_k": 2200,
      "target_brightness_pct": 80.0,
      "target_color_temp_k": 4000,
      "enabled": true
    }
  ]
}
```

Row fields:

| Field | Description |
|---|---|
| `time` | `HH:MM` local. |
| `days_of_week` | `0..6` = Mon..Sun; empty = every day. |
| `duration_s` | Ramp length. Passed to the lamp as Zigbee `transition`. |
| `start_brightness_pct` / `start_color_temp_k` | Snap-to state at `time` (0-s transition). Explicit so sunrise is deterministic regardless of where the bulb was. |
| `target_brightness_pct` / `target_color_temp_k` | End state after `duration_s`. |
| `enabled` | Set via `set_schedule_enabled`. |

Schedules fire within a 30-minute catch-up window after `HH:MM`; anything later is marked missed in the state file. A ramp that starts within that window fully respects `duration_s` — it does not shorten.

### do_command

| Command | Payload | Returns |
|---|---|---|
| `status` | — | `{kind, health, message, mqtt_connected, device_available, last_state_at, last_cmd_result, readings, schedules}` |
| `get` | — | `{state, brightness_pct, color_temp_k}` after a fresh `/get` round-trip. |
| `set` | `{on?, brightness_pct?, color_temp_k?, transition_s?}` | `{ok: true}` once the state echo arrives. |
| `add_schedule` | `{schedule: {...}}` | `{ok: true, schedule: <normalized>}` |
| `modify_schedule` | `{schedule: {id, ...fields}}` | `{ok: true, schedule: <merged>}` |
| `delete_schedule` | `{id}` | `{ok: true, id}` |
| `set_schedule_enabled` | `{id, enabled}` | `{ok: true, id, enabled}` |

### Health and UI copy

`status.health` is a derived enum that distinguishes broker failure from Zigbee failure from physical power-off. `status.message` is the user-facing string for that enum, so frontends render it verbatim rather than reverse-engineering the enum.

| Health | Message | Meaning |
|---|---|---|
| `ok` | `""` | Broker connected, bulb available, last command succeeded. |
| `lamp_power_off` | `switched off at the lamp` | zigbee2mqtt marked the bulb `offline` on its availability topic — physical power cut. |
| `zigbee_unreachable` | `not responding` | Broker is up, but the last command timed out waiting for the bulb to echo. |
| `mqtt_down` | `broker offline` | The module's MQTT client is not connected to the broker. |
| `module_error` | `error` | Reserved for module-side crashes; frontends should treat as a catch-all. |

Requires `availability: true` in zigbee2mqtt's config so `lamp_power_off` can be distinguished from `zigbee_unreachable`. Without it the module falls back to the timeout path and reports `zigbee_unreachable` in both cases.

Tested with Philips Hue White and Color Ambiance A19 (E26), using the CT + brightness slice only.

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
