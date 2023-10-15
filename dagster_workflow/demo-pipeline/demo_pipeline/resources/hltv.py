import time
import re
import undetected_chromedriver as uc

from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
import undetected_chromedriver as uc
from pydantic import Field

from dagster import ConfigurableResource, get_dagster_logger

class HltvResource(ConfigurableResource):
    """Resource for scraping data from hltv.org."""

    results_page_offset: int = Field(
        description=(
            "The offset for the results page on hltv, is the number of results from the most recent result to start listing matches from."
        ),
        default=0
    )

    @property
    def driver(self) -> webdriver.Chrome:
        options = uc.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-dev-shm-usage")
        driver = uc.Chrome(options=options)
        return driver


    def scrape_match(self, match_url: str):
        HLTV_ADDR = "https://www.hltv.org"

        driver = self.driver # Sets it to own var as calls to self.driver will keep creating new instances
        match_id = re.search(r"\d+", match_url).group()
        driver.get(f"{HLTV_ADDR}{match_url}")
    
        # Wait for page to load if not fully loaded
        time.sleep(5)

        match_details = BeautifulSoup(driver.page_source, "html.parser")
        # get match_id from url
        try:
            team_box = match_details.find("div", class_="standard-box teamsBox")

            match_footnotes = match_details.find("div", class_="padding preformatted-text").text
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
        except:
            driver.close()
            demo_link = None

        try:
            # get demo id
            logger = get_dagster_logger()
            demo_a_tag = match_details.find("a", {"data-demo-link": True})

            demo_link = re.findall("\d+", demo_a_tag["data-demo-link"])[0]
            driver.close()
        except:
            driver.close()
            demo_link = None

        #type cast scores
        team_a_score = int(team_a_score)
        team_b_score = int(team_b_score)


        # bo1 so score will be the rounds won which will be 16 minimum
        maps_played = 1 if team_a_score + team_b_score >= 16 else team_a_score + team_b_score

        is_cs2 = "Counter-Strike 2" in match_footnotes

        match_data = {
            "hltv_id": int(match_id),
            "url": match_url,
            "team_a": team_a,
            "team_b": team_b,
            "team_a_score": team_a_score,
            "team_b_score": team_b_score,
            "competition": competition,
            "date": datetime.utcfromtimestamp(unix_datetime / 1000).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "demo_id": False if demo_link is None else int(demo_link),
            "maps_played": maps_played,
            "is_cs2": is_cs2,
        }

        return match_data     
