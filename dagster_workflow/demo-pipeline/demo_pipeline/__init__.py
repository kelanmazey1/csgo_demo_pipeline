from dagster import Definitions

from .assets import (
    core_assets
)

from .jobs.jobs_using_ops import testing

all_assets = [
    *core_assets
]

# Creating list even though probably only using jobs_using_ops,
# incase want to add other types in future
all_jobs = [
    testing
]

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs
)
