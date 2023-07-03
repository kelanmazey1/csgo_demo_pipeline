import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
subdirectory_path = os.path.join(current_dir, "functions")
sys.path.append(subdirectory_path)

# from dl_unzip import dl_unzip
from get_matches import get_match_urls
from scrape_match import get_match_details
from prefect import flow


@flow()
def download_and_parse():
    match_urls = get_match_urls()
    match_data = get_match_details.map(match_urls)
    print(match_data)


if __name__ == "__main__":
    download_and_parse()
