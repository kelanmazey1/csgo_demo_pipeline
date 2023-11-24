from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pathlib import Path
from dagster import (
    asset,
)

events = {
        "grenades": ["grenade", "grenade_position", "grenade_thrower", "grenade_thrower_pos", "grenade_type", "round"],
        "kills": ["head_shot", "killer", "killer_position", "victim", "victim_position", "through_smoke", "wallbang", "weapon", "round"],
        "player_damaged": ["armor", "armor_damage", "attacker", "attacker_pos", "player_hurt", "player_hurt_pos", "health", "health_damage", "hit_group","weapon", "round"],
        "shots_fired" : ["shooter", "shooter_pos", "weapon", "round"],
        "economy": ["round", "player", "money_spent", "non_armour_equipment_value", "armour", "helmet", "net_spend", "inventory", "team_money_spent", "team_total_equipment_value", "team", "team_game_id"],
    }


@asset(deps=["demo_jsons"], io_manager_key="spark_io_manager")
def json_df() -> DataFrame:
    # Get demos dir
    demos_dir = Path.cwd() / "demos"

    if not demos_dir.exists():
        raise FileNotFoundError("No demo's directory")
    spark = SparkSession.builder.getOrCreate()
    # paths = [str(path) for path in demos_dir.rglob("*.json")]

    dfs = []

    for path in demos_dir.rglob("*.json"):
        # TODO: Handle empty JSON, the parser ran without error but produced an empty json on match ID 82645
        # may have just been that demo corrupted or bug with library but still needs handling.
        df = spark.read.json(str(path))
        
        df = df.withColumn("filename", F.input_file_name()) \
            .withColumn("hltv_id", F.lit(str(path.parent))) \
            .withColumn("map", F.regexp_extract("filename", "-([^-]+)\.json", 1))

        dfs.append(df)

    result = reduce(DataFrame.unionAll, dfs)

    return result

def generate_staging_tables(event_type, sub_event_list):

    @asset(name=event_type, io_manager_key="spark_io_manager", key_prefix=["staging"])
    def _staging_table(json_df: DataFrame):

        cols_to_select = [f"{event_type}.{sub_topic}" for sub_topic in sub_event_list]
        topic_df = json_df.select(cols_to_select)
        topic_flat = topic_df.withColumn("zipped", F.arrays_zip(*sub_event_list)) \
                    .withColumn("zipped", F.explode("zipped")) \
                    .withColumn(f"{event_type}_id", F.monotonically_increasing_id()) \
                    .select([f"zipped.{sub_topic}" for sub_topic in sub_event_list]) 
            
        return topic_flat

    return _staging_table


staging_table_assets = [generate_staging_tables(event_type, sub_event_list) for event_type, sub_event_list in events.items()]
