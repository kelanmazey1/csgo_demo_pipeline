from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from pyspark.sql import functions as F
from pathlib import Path
from dagster import (
    asset,
    ConfigurableIOManager,
    get_dagster_logger,
    Output,
    Definitions,
)
    #      |-- grenades: array (nullable = true)
    #  |    |-- element: struct (containsNull = true)
    #  |    |    |-- grenade: string (nullable = true)
    #  |    |    |-- grenade_position: struct (nullable = true)
    #  |    |    |    |-- X: double (nullable = true)
    #  |    |    |    |-- Y: double (nullable = true)
    #  |    |    |    |-- Z: double (nullable = true)
    #  |    |    |-- grenade_thrower: string (nullable = true)
    #  |    |    |-- grenade_thrower_pos: struct (nullable = true)
    #  |    |    |    |-- X: double (nullable = true)
    #  |    |    |    |-- Y: double (nullable = true)
    #  |    |    |    |-- Z: double (nullable = true)
    #  |    |    |-- grenade_type: string (nullable = true)
    #  |    |    |-- round: long (nullable = true)
    #  |-- kills: array (nullable = true)
    #  |    |-- element: struct (containsNull = true)
    #  |    |    |-- head_shot: boolean (nullable = true)
    #  |    |    |-- killer: string (nullable = true)
    #  |    |    |-- killer_position: struct (nullable = true)
    #  |    |    |    |-- X: double (nullable = true)
    #  |    |    |    |-- Y: double (nullable = true)
    #  |    |    |    |-- Z: double (nullable = true)
    #  |    |    |-- round: long (nullable = true)
    #  |    |    |-- through_smoke: boolean (nullable = true)
    #  |    |    |-- victim: string (nullable = true)
    #  |    |    |-- victim_position: struct (nullable = true)
    #  |    |    |    |-- X: double (nullable = true)
    #  |    |    |    |-- Y: double (nullable = true)
    #  |    |    |    |-- Z: double (nullable = true)
    #  |    |    |-- wallbang: boolean (nullable = true)
    #  |    |    |-- weapon: string (nullable = true)
    #  |-- player_damaged: array (nullable = true)
    #  |    |-- element: struct (containsNull = true)
    #  |    |    |-- armor: long (nullable = true)
    #  |    |    |-- armor_damage: long (nullable = true)
    #  |    |    |-- attacker: string (nullable = true)
    #  |    |    |-- attacker_pos: struct (nullable = true)
    #  |    |    |    |-- X: double (nullable = true)
    #  |    |    |    |-- Y: double (nullable = true)
    #  |    |    |    |-- Z: double (nullable = true)
    #  |    |    |-- health: long (nullable = true)
    #  |    |    |-- health_damage: long (nullable = true)
    #  |    |    |-- hit_group: long (nullable = true)
    #  |    |    |-- player_hurt: string (nullable = true)
    #  |    |    |-- player_hurt_pos: struct (nullable = true)
    #  |    |    |    |-- X: double (nullable = true)
    #  |    |    |    |-- Y: double (nullable = true)
    #  |    |    |    |-- Z: double (nullable = true)
    #  |    |    |-- round: long (nullable = true)
    #  |    |    |-- weapon: string (nullable = true)
    #  |-- shots_fired: array (nullable = true)
    #  |    |-- element: struct (containsNull = true)
    #  |    |    |-- round: long (nullable = true)
    #  |    |    |-- shooter: string (nullable = true)
    #  |    |    |-- shooter_pos: struct (nullable = true)
    #  |    |    |    |-- X: double (nullable = true)
    #  |    |    |    |-- Y: double (nullable = true)
    #  |    |    |    |-- Z: double (nullable = true)
    #  |    |    |-- weapon: string (nullable = true)
    
@asset(deps=["demo_jsons"], io_manager_key="spark_io_manager")
def demo_long_staging() -> DataFrame:
    # Get demos dir
    demos_dir = Path.cwd() / "demos"
    
    if not demos_dir.exists():
        raise FileNotFoundError("No demo's directory")

    spark = SparkSession.builder.getOrCreate()


    
    for f in demos_dir.rglob("*.json"):
        df = spark.read.json(str(f))

    # Have keys as main topics ie. nades, kills and sub cols as list of vals to unpack when array zip
    json_dict = {
        "grenades": ["grenade", "grenade_position", "grenade_thrower", "grenade_thrower_pos", "grenade_type", "round"],
    }

    for main_topic, sub_topic_list in json_dict.items():
        # This is so we can read subcols for main topic, only need one level deep. In theory could be done recursively but wasn't worth the effor to work out how to traverse schema
        cols_to_select = [f"{main_topic}.{sub_topic}" for sub_topic in sub_topic_list]
        sub_df = df.select(cols_to_select)
        sub_flat = sub_df.withColumn("zipped", F.arrays_zip(*sub_topic_list)) \
                  .withColumn("zipped", F.explode("zipped")) \
                  .select([f"zipped.{sub_topic}" for sub_topic in sub_topic_list])
        sub_flat.show()

    
    
    return df

