from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pathlib import Path
from dagster import (
    asset,
)
    
@asset(deps=["demo_jsons"], io_manager_key="db_manager", key_prefix=["staging"])
def demo_long_staging():
    # Get demos dir
    demos_dir = Path.cwd() / "demos"
    
    if not demos_dir.exists():
        raise FileNotFoundError("No demo's directory")

    spark = SparkSession.builder.getOrCreate()
    
    for f in demos_dir.rglob("*.json"):
        # TODO: Handle empty JSON, the parser ran without error but produced an empty json on match ID 82645
        # may have just been that demo corrupted or bug with library but still needs handling.
        df = spark.read.json(str(f))
 
    json_dict = {
        "grenades": ["grenade", "grenade_position", "grenade_thrower", "grenade_thrower_pos", "grenade_type", "round"],
        "kills": ["head_shot", "killer", "killer_position", "victim", "victim_position", "through_smoke", "wallbang", "weapon", "round"],
        "player_damaged": ["armor", "armor_damage", "attacker", "attacker_pos", "player_hurt", "player_hurt_pos", "health", "health_damage", "hit_group","weapon", "round"],
        "shots_fired" : ["shooter", "shooter_pos", "weapon", "round"],
        "economy": ["round", "player", "money_spent", "non_armour_equipment_value", "armour", "helmet", "net_spend", "inventory", "team_money_spent", "team_total_equipment_value", "team", "team_game_id"],
    }

    dataframe_list = []

    event_id_counter = 0

    for main_topic, sub_topic_list in json_dict.items():
        # This is so we can read subcols for main topic, only need one level deep. In theory could be done recursively but wasn't worth the effor to work out how to traverse schema
        cols_to_select = [f"{main_topic}.{sub_topic}" for sub_topic in sub_topic_list]
        sub_df = df.select(cols_to_select)
        sub_flat = sub_df.withColumn("zipped", F.arrays_zip(*sub_topic_list)) \
                  .withColumn("zipped", F.explode("zipped")) \
                  .withColumn(f"event_id", F.monotonically_increasing_id() + 1 + event_id_counter) \
                  .select([f"zipped.{sub_topic}" for sub_topic in sub_topic_list] + [f"event_id"]) \
        
        sub_flat = sub_flat.unpivot(ids=["round"], values=sub_topic_list, variableColumnName="event_type", valueColumnName="value")
        sub_flat.show()
        
        
        event_id_counter = sub_flat.agg({"event_id": "max"}).collect()[0]




    return df

