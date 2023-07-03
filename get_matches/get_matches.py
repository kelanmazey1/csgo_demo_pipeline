import re, requests, argparse, json
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, List, Tuple
import undetected_chromedriver as uc

# Create web driver - uses undected to avoid cloudflare
options = uc.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--disable-dev-shm-usage")
driver = uc.Chrome(options=options)
HLTV_ADDR = "https://www.hltv.org"

driver.get(HLTV_ADDR)
print(f"did it work: {driver.title}")


def main(args):
    offset = int(args.offset)
    # Generate list of offset values, 0 always included as this is the final results page
    offset_values = [0]
    while offset > 0:
        offset_values.append(offset)
        # Use 100 as this is equivalent to going to the next page on hltv
        offset -= 100

    # Get match URLS, ~100 matches per page offset decreases by 100 until most recent page ie. offset=100
    match_urls = []
    for offset in offset_values:
        match_urls += get_match_urls(offset=offset)
        # remove duplicate matches
        match_urls = list(set(match_urls))

    driver.close()
    return match_urls


def get_match_urls(offset: int) -> List[str]:
    URL = f"{HLTV_ADDR}/results?offset={offset}"
    print(f"Getting matches from {URL}")
    # Get HTML doc for beautiful soup
    results_page = requests.get(URL)

    hltv_results = BeautifulSoup(results_page.text, "html.parser")
    results = []
    # Pull match link from html
    for a in hltv_results.find_all("a", class_="a-reset"):
        link = str(a.get("href"))

        if link.startswith("/matches/"):
            results.append(link)
    return results
