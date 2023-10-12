from dagster import op, job
from ..utils import dl_unzip

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

@op
def do_something():
    return 5

@op
def wow(num):
    return num + 1


@job
def testing():
    wow(do_something())
