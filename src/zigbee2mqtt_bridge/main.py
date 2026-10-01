"""Viam module entrypoint for joseph:zigbee2mqtt:bridge."""

import asyncio

from viam.components.sensor import Sensor
from viam.module.module import Module

from . import bridge  # noqa: F401  — registers Bridge with the Registry


async def main() -> None:
    module = Module.from_args()
    module.add_model_from_registry(Sensor.API, bridge.Bridge.MODEL)
    await module.start()


if __name__ == "__main__":
    asyncio.run(main())
