import subprocess
import os
import pandas as pd
from ...resources import HltvResource
from typing import List, Dict
from demo_pipeline.utils.get_matches import get_match_urls
from demo_pipeline.utils.dl_unzip import dl_unzip

from dagster import (
    asset,
    op,
    Output,
    Out,
    graph_asset,
    multi_asset,
    AssetOut,
    AssetExecutionContext,
    get_dagster_logger,
)

@asset(io_manager_key="fs_io_manager")
def matches_on_results_page() -> Output[List[str]]:
    logger = get_dagster_logger()
    results = get_match_urls()[:20]
    logger.info(results)
    return Output(
        value=results, metadata={"num_matches": len(results), "preview": results[:5]}
    )

# Creating assets of failed and successful, ie. demo link provided. HLTV can be flakey no demo link, page is layed out differently or could just be timeout.
# Adding second asset of fails can allow for final retries or at least a view of what has failed.
@multi_asset(
    outs={
        "successful_scrapes": AssetOut(),
        "failed_scrapes": AssetOut()
    }
)
def get_details_of_match(
    context: AssetExecutionContext,
    hltv_scraper: HltvResource,
    matches_on_results_page: List[str]
):
    logger = get_dagster_logger()
    # try get match details, can fail for a number of reasons, if fails log and retry
    # TODO: attempt to add to queue for review, try make multi asset, failed queue succeeded queue
    success = []
    failed = []
    for match in matches_on_results_page:
        # skip demo if is_cs2 as parser is not ready
        match_data = (hltv_scraper.scrape_match(match))

        if match_data["demo_id"] is not None:
            success.append(match_data)
        else:
            failed.append(match_data)
        
        logger.info(match_data)

    return (Output(success, output_name="successful_scrapes", metadata={"number_of_success": len(success), "preview": success[:5]}), 
        Output(failed, output_name="failed_scrapes", metadata={"number_of_fails": len(failed), "preview": failed[:5]}))


@op
def demo_download(match) -> str:
    if not match["is_cs2"]:
        return dl_unzip(match)


# # TODO: Actually just return a single JSON file, as the file is the asset 
# @op(
#     out=Out(
#         io_manager_key="fs_io_manager",
#         is_required=True,
#         )
# )
# def parse_json(demo_dir: str) -> List[pd.DataFrame]:
#     output = []
#     # Run golang parser
#     for file_path in demo_dir:
#         for file in os.listdir(file_path):
#             demo_path = os.file_path.join(file_path, file)
#             print(demo_path)
#             subprocess.run(
#                     ["./demo_pipeline/utils/demo_parse/parse_demo", demo_path, "./"]
#                 )
#             # TODO: store JSONs somewhere after parse as currently overwrites each time in loop
#             # thinking best way is something like duckdb for local? Maybe just to file storage
#             df = pd.read_json("./data/output.json")
#             output.append(df)

#     # Return something like, json.loads(output.json)
#     return output

# This can be the spark job?? For everything in get match_details / successful scrape
# Read into big df and write to table
@graph_asset
def json_table(successful_scrapes: List[Dict]) -> List[str]:
    return parse_json(demo_download(successful_scrapes))
