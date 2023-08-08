import os
import subprocess
import json
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
    results = get_match_urls()[:1]
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
def demo_download(match_details):
    # Make working dir
    cwd = os.getcwd()
    
    os.mkdir('work')
    work_dir = os.path.join(cwd, 'work')

    for match in match_details:
        # Change back to work_dir for each match
        os.chdir(work_dir)
        dl_unzip(match)

    # Return match_details with demo download location added

@asset
def demo_parsed_json(match_details, demo_download):
    # make empy output fie to append to
    open("all_maps.json", "a").close()

    # Parse each match as Json then append to final output 
    for match in match_details:
        
        demos_folder = f"./work/{match['demo_id']}/demo_files/"

        if not os.path.exists(demos_folder):
            raise FileExistsError("Work dir does not exist!")
        
        final_output = []

        for count, demo in enumerate(os.listdir(demos_folder)):
            demo_path = os.path.join(os.getcwd(), demos_folder, demo)
            subprocess.run(["./demo_pipeline/functions/demo_parse/parse_demo", demo_path, "./"])
           

            with open("output.json") as o:
                output_obj = json.load(o)

            final_output.append(output_obj)
    

        with open("all_maps.json", "w") as all_maps_out:
            json.dump(final_output, all_maps_out)

    # Go to dir for demo_id

    # Iterate through each demo and write out json for match events

    # Attach events json to other match details

