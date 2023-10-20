from dagster import (
    Definitions,
    FilesystemIOManager
)

from .resources import HltvResource

from .assets import (
    core_assets
)

all_assets = [
    *core_assets
]

fs_io_manager = FilesystemIOManager(
    base_dir="data",
)

hltv = HltvResource(results_page_offset=1000)

defs = Definitions(
    assets=all_assets,
    resources={
        "fs_io_manager": fs_io_manager,
        "hltv_scraper": hltv,
    }   
)
