from dagster import Definitions, load_assets_from_package_module

from . import demo_collection, load_demo

demo_collection_assets = load_assets_from_package_module(package_module=demo_collection, group_name="demo_collection")
load_demo_assets = load_assets_from_package_module(package_module=load_demo, group_name="load_demo")

