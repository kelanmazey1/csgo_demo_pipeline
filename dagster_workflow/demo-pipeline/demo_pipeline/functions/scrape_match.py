import re, sys, time
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from typing import Dict, List, Tuple
import undetected_chromedriver as uc


def get_match_details(match_url: str) -> List[Dict]:
    # Create web driver - uses undected to avoid cloudflare
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    driver = uc.Chrome(options=options)
    HLTV_ADDR = "https://www.hltv.org"
    
    match_id = re.search(r"\d+", match_url).group()
    driver.get(f"{HLTV_ADDR}{match_url}")
    
    # Wait for page to load if not fully loaded
    time.sleep(5)

    print(f"Scraping: {driver.title}")
    match_details = BeautifulSoup(driver.page_source, "html.parser")
    # get match_id from url
    try:
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
    except AttributeError as e:
        driver.close()
        raise AttributeError(f"Couldn't get details from match {match_url}, original error: {e}")
    
    try:
        # get demo id
        demo_a_tag = match_details.find("a", {"data-demo-link": True})
        demo_link = re.findall("\d+", demo_a_tag["data-demo-link"])[0]
        driver.close()
    except AttributeError as e:
        driver.close()
        raise AttributeError(f"Couldn't find a demo ID for {match_url}, original error: {e}")
        
    #type cast scores
    team_a_score = int(team_a_score)
    team_b_score = int(team_b_score)


    # bo1 so score will be the rounds won in a map else could be bo2, bo3 etc.
    maps_played = 1 if team_a_score + team_b_score > 3 else team_a_score + team_b_score

    match_data = {
        "hltv_id": int(match_id),
        "url": match_url,
        "team_a": team_a,
        "team_b": team_b,
        "team_a_score": team_a_score,
        "team_b_score": team_b_score,
        "competition": competition,
        "date": datetime.utcfromtimestamp(unix_datetime / 1000).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "demo_id": int(demo_link),
        "maps_played": maps_played,
    }

    return match_data


if __name__ == "__main__":
    sys.exit(0)
