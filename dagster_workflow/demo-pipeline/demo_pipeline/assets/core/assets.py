import subprocess
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from demo_pipeline.utils.get_matches import get_match_urls
from demo_pipeline.utils.scrape_match import get_match_details
from demo_pipeline.utils.dl_unzip import dl_unzip
from dagster import asset, Definitions, job, op, Output, RetryRequested, graph_asset, multi_asset, AssetOut


# TODO: Not currently sure how this will work in current workflow
# Will currently interact with host file system, will be S3 in cloud
# class StorageIOManager(ConfigurableIOManager):
#     def load_input(self, context: InputContext) -> Any:
#         return super().load_input(context)

#     def handle_output(self, context: OutputContext, obj: Any) -> None:
#         return super().handle_output(context, obj)


@asset
def matches_on_results_page():
    results = get_match_urls()[:30]
    print(results)
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
def get_details_of_match(matches_on_results_page):
    # try get match details, can fail for a number of reasons, if fails log and retry
    # TODO: attempt to add to queue for review, try make multi asset, failed queue succeeded queue
    success = []
    failed = []
    for match in matches_on_results_page:
        try:
            success.append(get_match_details(match))
        except Exception:
            # get hltv id if fails
            print("IT'S IN THE FAILED PART")
            failed.append({
                "match_url": match.rsplit('/', 1)[-1],
                "demo_id": None,
                "message": "download error"
                })
            # raise RetryRequested(max_retries=5) from e
    return (Output(success, output_name="successful_scrapes", metadata={"number_of_success": len(success), "preview": failed[:5]}), 
        Output(failed, output_name="failed_scrapes", metadata={"number_of_fails": len(failed), "preview": failed[:5]}))

@op
def demo_download(match_details) -> None:
    # Make working dir
    # cwd = os.getcwd()

    # os.mkdir("work")
    # work_dir = os.path.join(cwd, "work")

    # os.chdir(work_dir)
    for match in match_details:
        dl_unzip(match)

    # Return match_details with demo download location added

@op
def parse_json(demo):
    # # Run golang parser
    # subprocess.run(
    #     ["./demo_pipeline/functions/demo_parse/parse_demo", demo_path, "./"]
    # )
    pass

@job
def download_demos():
    demo_download()

@op
def return_five():
    return 5


@op
def add_one(arg):
    return arg + 1


@job
def do_stuff():
    add_one(return_five())

# @graph_asset
# def parse_demo():
#     return get_details_of_match(matches_on_current_results_page())

# @asset
# def my_giant_json(parse_jsons):
#     # TODO: id 
#     return already_existing_json.append(parse_json(demo_file))

# @asset
# def demo_parsed_json(match_details, demo_download):
#     # make empy output fie to append to
#     open("all_maps.json", "a").close()

#     # Parse each match as Json then append to final output
#     for match in match_details:
#         demos_folder = f"./work/{match['demo_id']}/demo_files/"

#         if not os.path.exists(demos_folder):
#             raise FileExistsError("Work dir does not exist!")

#         final_output = []

#         for count, demo in enumerate(os.listdir(demos_folder)):
#             demo_path = os.path.join(os.getcwd(), demos_folder, demo)
#             

#             with open("output.json") as o:
#                 output_obj = json.load(o)

#             final_output.append(output_obj)

#         with open("all_maps.json", "w") as all_maps_out:
#             json.dump(final_output, all_maps_out)

    # Go to dir for demo_id

    # Iterate through each demo and write out json for match events

    # Attach events json to other match details
