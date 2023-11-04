from dagster import (
    Definitions,
    FilesystemIOManager,
    ConfigurableIOManager
)

from pyspark.sql import SparkSession, DataFrame
import os
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

class LocalParquetIOManager(ConfigurableIOManager):
    def _get_path(self, context):
        return os.path.join('./demos', *context.asset_key.path)

    def handle_output(self, context, obj: DataFrame):
        obj.write.parquet(self._get_path(context), mode="overwrite")

    def load_input(self, context):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))

hltv = HltvResource(results_page_offset=1000, single_results_page=True)

defs = Definitions(
    assets=all_assets,
    resources={
        "fs_io_manager": fs_io_manager,
        "hltv_scraper": hltv,
        "spark_io_manager": LocalParquetIOManager()
        # In prod it will be like read json from s3 bucket 
        # In dev it's read from demos folder
    }   
)
