from typing import Any, Dict, List
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from demo_pipeline.functions.get_matches import get_match_urls 
from demo_pipeline.functions.scrape_match import get_match_details
from demo_pipeline.functions.dl_unzip import dl_unzip
from dagster import asset, Definitions, define_asset_job, op, Output, RetryPolicy
from dagster import ConfigurableIOManager

# TODO: Not currently sure how this will work in current workflow
# Will currently interact with host file system, will be S3 in cloud
# class StorageIOManager(ConfigurableIOManager):
#     def load_input(self, context: InputContext) -> Any:
#         return super().load_input(context)
    
#     def handle_output(self, context: OutputContext, obj: Any) -> None:
#         return super().handle_output(context, obj)

@asset
def matches_on_current_results_page():
    results = get_match_urls()[:10]
    print(results)
    return Output(
        value=results,
        metadata={
            "num_matches": len(results),
            "preview": results[:5]
        })

@asset(retry_policy=RetryPolicy(max_retries=3))
def match_details(matches_on_current_results_page):
    match_data = []
    for match in matches_on_current_results_page:
        match_data.append(get_match_details(match))
    
    print(match_data)

    return Output(
        value=match_data,
        metadata={
            "num_matches": len(match_data),
            "preview": match_data[:1]
        })

@asset
def demo_download(match_details) -> None:
    for match in match_details:
        dl_unzip(match)


