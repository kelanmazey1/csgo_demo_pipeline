import re, sys
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, List, Tuple
import undetected_chromedriver as uc
from prefect import task, get_client

# Create web driver - uses undected to avoid cloudflare
options = uc.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--disable-dev-shm-usage")
driver = uc.Chrome(options=options)
HLTV_ADDR = "https://www.hltv.org"


@task(tags=["scrape_match"])
def get_match_details(match_url: str):
    # TODO: Add try catch and handle if info isn't available on the page

    # get match_id from url
    match_id = re.search(r"\d+", match_url).group()
    driver.get(f"{HLTV_ADDR}{match_url}")
    print(f"did it work: {driver.title}")
    match_details = BeautifulSoup(driver.page_source, "html.parser")
    team_box = match_details.find("div", class_="standard-box teamsBox")
    # Get team
    team_a_div = team_box.find("div", class_="team1-gradient")
    team_a = team_a_div.find("div").text
    team_b_div = team_box.find("div", class_="team2-gradient")
    team_b = team_b_div.find("div").text
    # 2 divs with name so de-duping
    team_a_score = team_a_div.find("div", class_=["won", "lost"]).text
    team_b_score = team_b_div.find("div", class_=["won", "lost"]).text
    # get competition from page
    competition_div = team_box.find("div", class_="event text-ellipsis")
    competition = competition_div.find("a").text
    # get date and cast from page
    unix_datetime_div = team_box.find("div", {"class": "time", "data-unix": True})
    unix_datetime = int(unix_datetime_div["data-unix"])

    # get demo id
    demo_a_tag = match_details.find("a", {"data-demo-link": True})
    demo_link = re.findall("\d+", demo_a_tag["data-demo-link"])[0]

    match_data = {
        "hltv_id": int(match_id),
        "url": match_url,
        "team_a": team_a,
        "team_b": team_b,
        "team_a_score": int(team_a_score),
        "team_b_score": int(team_b_score),
        "competition": competition,
        "date": unix_datetime,
        "demo_id": int(demo_link),
    }

    return match_data


if __name__ == "__main__":
    sys.exit(0)
