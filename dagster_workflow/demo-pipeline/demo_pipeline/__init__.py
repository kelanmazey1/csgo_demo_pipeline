from dagster import (
    Definitions,
    FilesystemIOManager
)

from .resources import HltvResource

from .assets import (
    demo_collection_assets,
    load_demo_assets,
)

all_assets = [
    *demo_collection_assets,
    *load_demo_assets,
]

fs_io_manager = FilesystemIOManager(
    base_dir="data",
)

hltv = HltvResource(results_page_offset=1000, single_results_page=True)

defs = Definitions(
    assets=all_assets,
    resources={
        "fs_io_manager": fs_io_manager,
        "hltv_scraper": hltv,
    }   
)
