"""Viam module entrypoint for viam:zigbee2mqtt:bridge and viam:zigbee2mqtt:switch."""

import asyncio

from viam.components.sensor import Sensor
from viam.components.switch import Switch
from viam.module.module import Module

from . import bridge, switch  # noqa: F401  — registers models with the Registry


async def main() -> None:
    module = Module.from_args()
    module.add_model_from_registry(Sensor.API, bridge.Bridge.MODEL)
    module.add_model_from_registry(Switch.API, switch.OnOffSwitch.MODEL)
    await module.start()


if __name__ == "__main__":
    asyncio.run(main())
