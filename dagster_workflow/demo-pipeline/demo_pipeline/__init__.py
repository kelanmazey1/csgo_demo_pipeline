from dagster import Definitions

from .assets import (
    core_assets
)

all_assets = [
    *core_assets
]

defs = Definitions(
    assets=all_assets,
)
