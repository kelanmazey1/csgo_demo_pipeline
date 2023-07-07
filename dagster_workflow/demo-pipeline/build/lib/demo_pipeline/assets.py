from typing import Any, Dict
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from demo_pipeline.functions.get_matches import get_match_urls 
from demo_pipeline.functions.scrape_match import get_match_details
from dagster import asset, op
from dagster import ConfigurableIOManager

# TODO: Not currently sure how this will work in current workflow
# Will currently interact with host file system, will be S3 in cloud
# class StorageIOManager(ConfigurableIOManager):
#     def load_input(self, context: InputContext) -> Any:
#         return super().load_input(context)
    
#     def handle_output(self, context: OutputContext, obj: Any) -> None:
#         return super().handle_output(context, obj)

# @asset()
# def pipeline():
#     get_matches()
#     scrape_match()
#     dl_unzip()

@asset()
def matches_on_current_results_page():
    results = get_match_urls()[:10]
    print(results)
    return results

# @op
# def return_match_details(match_url: str) -> Dict:

@asset()
def match_details(matches_on_current_results_page):
    matches_data = []
    
    for match in matches_on_current_results_page:
        matches_data.append(get_match_details(match))

    return matches_data

