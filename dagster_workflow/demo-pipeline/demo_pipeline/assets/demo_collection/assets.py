import subprocess

import patoolib
import pandas as pd
from ...resources import HltvResource
from pathlib import Path
from typing import List, Dict, Any

from dagster import (
    asset,
    Output,
    multi_asset,
    AssetOut,
    AssetExecutionContext,
    get_dagster_logger,
)

@asset(
    io_manager_key="fs_io_manager",
)
def matches_on_results_page(hltv_scraper: HltvResource) -> Output[List[str]]:
    logger = get_dagster_logger()
    results = hltv_scraper.get_results(num_of_results=1)
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
def match_details(
    hltv_scraper: HltvResource,
    matches_on_results_page: List[str]
):
    # try get match details, can fail for a number of reasons, if fails log and retry
    success = []
    failed = []
    for match in matches_on_results_page:
        # skip demo if is_cs2 as parser is not ready
        match_data = (hltv_scraper.scrape_match(match))

        if match_data["demo_id"] is not None:
            success.append(match_data)
        else:
            failed.append(match_data)

    return (Output(success, output_name="successful_scrapes", metadata={"number_of_success": len(success), "preview": success[:5]}), 
        Output(failed, output_name="failed_scrapes", metadata={"number_of_fails": len(failed), "preview": failed[:5]}))

# TODO: attempt to add to queue for review, try make multi asset, failed queue succeeded queue
@asset
def retried_scrapes(failed_scrapes):
    pass

@asset
def demo_archives(successful_scrapes: List[Dict[str, Any]], hltv_scraper: HltvResource):
    """ Returns a list of paths to directories containing a .rar archive of demos from a csgo match """
    logger = get_dagster_logger()
    home_dir = Path.cwd()
    archive_paths = []
    
    for scrape in successful_scrapes:
        demo_id = scrape["demo_id"]
        
        archive_dir = home_dir / 'demos' / str(demo_id)
        archive_dir.mkdir(parents=True, exist_ok=False)
        hltv_scraper.download_demos(demo_id, outdir=archive_dir.resolve())
        logger.info(f"downloading {scrape['team_a']} vs {scrape['team_b']}")
        
        archive_paths.append(archive_dir.resolve())

    # Removes old chromedrivers brought in from scraper
    hltv_scraper.clean_up_chromedrivers()
    
    return archive_paths

@asset
def demo_jsons(demo_archives: List[Path]) -> None:
    """ Uses go script to parse demo files to json, leaves a json in match directory renamed to match and map """
    logger = get_dagster_logger()
    # Read in .rar file with io
    for archive_path in demo_archives:
        rar_files = [x for x in archive_path.glob("*.rar")]
        if len(rar_files) > 1:
            raise FileExistsError("More than 2 .rar files have been found, expected only 1")

        # Keep everything pertaining to one match in same dir
        host_directory = archive_path.resolve()

        # Unzip archive
        patoolib.extract_archive(rar_files[0], outdir=host_directory)

        for demo_file in archive_path.glob("*.dem"):
            
            output_path = demo_file.parent.resolve() / demo_file.stem
            logger.info(f"{demo_file.resolve()} being processed")
            subprocess.run(
                    ["./demo_pipeline/utils/demo_parse/parse_demo", demo_file.resolve(), output_path.with_suffix(".json")]
                )

            # Delete demo file after parsed to json
            demo_file.unlink()
        # Delete archive once done
        rar_files[0].unlink()
