import os
from dagster import (
    Definitions,
    FilesystemIOManager,
    ConfigurableIOManager
)

from dagster_duckdb_pyspark import DuckDBPySparkIOManager
from pyspark.sql import SparkSession, DataFrame
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
        return os.path.join('./data', *context.asset_key.path)

    def handle_output(self, context, obj: DataFrame):
        obj.write.parquet(self._get_path(context), mode="overwrite")

    def load_input(self, context):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))

hltv = HltvResource(results_page_offset=1000, single_results_page=True)

resources = {
    "local": {
        "fs_io_manager": fs_io_manager,
        "hltv_scraper": hltv,
        "spark_io_manager": LocalParquetIOManager(),
        "db_manager": DuckDBPySparkIOManager(database="counter_strike.db")
    },
    "production": {
        "fs_io_manager": fs_io_manager,
        "hltv_scraper": hltv,
        # TODO: Set this as S3 and RDS in AWS
        "spark_io_manager": LocalParquetIOManager(),
        "db_manager": DuckDBPySparkIOManager(database="counter_strike.db")
    }   
}

deployment = os.getenv("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    resources=resources[deployment]
)
