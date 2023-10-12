from dagster import Definitions, loa

from . import core

core_assets = load_assets_from_package_module(package_module=core, group_name="core")

