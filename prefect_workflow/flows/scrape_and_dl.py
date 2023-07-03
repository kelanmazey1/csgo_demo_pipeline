import os, sys

sys.path.append("../functions")

import dl_unzip
import get_matches
import scrape_match
from prefect import task, flow


@task()
@flow()
def download_and_parse():
    print("hello")


download_and_parse()
