import sys
from bs4 import BeautifulSoup
from typing import List
import undetected_chromedriver as uc

# Create web driver - uses undected to avoid cloudflare
options = uc.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--disable-dev-shm-usage")
driver = uc.Chrome(options=options)
HLTV_ADDR = "https://www.hltv.org"


def get_match_urls(offset=0) -> List[str]:
    driver.get(HLTV_ADDR)
    print(f"did it work: {driver.title}")
    # Generate list of offset values, 0 always included as this is the final results page
    offset_values = [0]
    while offset > 0:
        offset_values.append(offset)
        # Use 100 as this is equivalent to going to the next page on hltv
        offset -= 100

    # Get match URLS, ~100 matches per page offset decreases by 100 until most recent page ie. offset=100
    match_urls = []
    for offset in offset_values:
        match_urls += scrape_results(offset=offset)
        # remove duplicate matches
        match_urls = list(set(match_urls))

    driver.close()
    return match_urls


def scrape_results(offset: int) -> List[str]:
    URL = f"{HLTV_ADDR}/results?offset={offset}"
    print(f"Getting matches from {URL}")
    # Get HTML doc for beautiful soup
    driver.get(URL)
    hltv_results = BeautifulSoup(driver.page_source, "html.parser")
    # hltv_results = BeautifulSoup(results_page.text, "html.parser")
    results = []
    # Pull match link from html
    for a in hltv_results.find_all("a", class_="a-reset"):
        link = str(a.get("href"))

        if link.startswith("/matches/"):
            results.append(link)
    return results


if __name__ == "__main__":
    sys.exit(0)
