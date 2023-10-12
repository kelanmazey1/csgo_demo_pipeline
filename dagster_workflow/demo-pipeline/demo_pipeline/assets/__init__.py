from dagster import Definitions, load_assets_from_package_module

from . import core

core_assets = load_assets_from_package_module(package_module=core, group_name="core")

