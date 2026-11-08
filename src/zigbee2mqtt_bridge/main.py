"""Viam module entrypoint for viam:zigbee2mqtt:{bridge,switch,light}."""

import asyncio

from viam.components.generic import Generic
from viam.components.sensor import Sensor
from viam.components.switch import Switch
from viam.module.module import Module

from . import bridge, light, switch  # noqa: F401  — registers models with the Registry


async def main() -> None:
    module = Module.from_args()
    module.add_model_from_registry(Sensor.API, bridge.Bridge.MODEL)
    module.add_model_from_registry(Switch.API, switch.OnOffSwitch.MODEL)
    module.add_model_from_registry(Generic.API, light.Light.MODEL)
    await module.start()


if __name__ == "__main__":
    asyncio.run(main())
