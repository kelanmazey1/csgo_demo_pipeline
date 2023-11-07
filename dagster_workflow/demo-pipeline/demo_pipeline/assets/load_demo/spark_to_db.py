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

@asset(deps=["demo_jsons"], io_manager_key="spark_io_manager")
def demo_long_staging() -> DataFrame:
    # Get demos dir
    demos_dir = Path.cwd() / "demos"
    
    if not demos_dir.exists():
        raise FileNotFoundError("No demo's directory")

    spark = SparkSession.builder.getOrCreate()
    files_to_read = []
    for f in demos_dir.rglob("*.json"):
        files_to_read.append(str(f.resolve()))
    
    df = spark.read.json(files_to_read)

    print(df.schema.fields)

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
    
    # Do transform, tag match ID on. Transform dataframe to wide format
    # write out to duckdb with built in duckdbpyspark manager

    # nades = df.select(
#     df["grenades.grenade"],
#     df["grenades.grenade_position"],
#     df["grenades.grenade_thrower"],
#     df["grenades.grenade_thrower_pos"],
#     df["grenades.grenade_type"],
#     df["grenades.round"],
#     )
# #e flattened data
# nades.show()

# nades_flat = nades.withColumn("new", F.arrays_zip("grenade", "grenade_position", "grenade_thrower", "grenade_thrower_pos", "grenade_type", "round")) \
#                   .withColumn("new", F.explode("new")) \
#                   .select(
#                       F.col("new.grenade").alias("grenade"),
#                       F.col("new.grenade_position").alias("grenade_position"),
#                       F.col("new.grenade_thrower").alias("grenade_thrower"),
#                       F.col("new.grenade_thrower_pos").alias("grenade_thrower_pos"),
#                       F.col("new.grenade_type").alias("grenade_type"),
#                       F.col("new.round").alias("round"),
#                       )

# nades_flat.show()

    
    return df

