from pathlib import Path
from pyspark.sql import SparkSession
from dagster import (
    asset,
    ConfigurableIOManager,
    get_dagster_logger,
    Output,
)

# from ..demo_collection.assets import demo_jsons

class LocalParquetIOManager(ConfigurableIOManager):
    """ Example from dagster, allows for reading and writing parquet files into memory as Spark dataframes """
    def _get_path(self, context):
        return Path(*context.asset_key.path)

    def handle_output(self, context, obj):
        obj.write.parquet(self._get_path(context))

    def load_input(self, context):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.parquet(self._get_path(context.upstream_output))

# Using this file to build a spark job to load 
@asset(deps=["demo_jsons"]) 
def demo_spark_df():
    """ Creates initial Spark DF from jsons in demos directory """

    logger = get_dagster_logger()
    demos_dir = Path.cwd() / "demos"
    spark = SparkSession.builder.getOrCreate()
    # Each json file is read and then appended as the demo_id needs to gathered to link back to match data
    demo_jsons = [x for x in demos_dir.rglob("*.json")]

    # output = spark.createDataFrame([], StructType([]))


    # for json in demo_jsons:
    #     logger.inf(json.resolve())
    #     df = spark.read.json(json.resolve())
    #     logger.info(df.show(5))
    #     logger.info(df.printSchema())
    # df = spark.read.json(demo_jsons[0].resolve())
    return Output(
        demo_jsons,
        metadata={"preview": demo_jsons, "schema": len(demo_jsons)}
        )

# defs = Definitions(
#     assets=[placeholder], resources={"io_manager": LocalParquetIOManager()}
# )
