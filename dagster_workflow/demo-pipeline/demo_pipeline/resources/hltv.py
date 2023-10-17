import time
import re
import os
import undetected_chromedriver as uc

from datetime import datetime
from typing import List
from pathlib import Path

from bs4 import BeautifulSoup
from selenium import webdriver
import undetected_chromedriver as uc
from pydantic import Field

from dagster import (
    ConfigurableResource,
    get_dagster_logger
    )

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

    @property
    def base_url(self) -> str:
        return "https://www.hltv.org"

    def scrape_results(self, offset: int, browser_session: webdriver.Chrome) -> List[str]:
        browser_session = self.driver # Sets it to own var as calls to self.driver will keep creating new instances
        URL = f"{self.base_url}/results?offset={offset}"
        # Get HTML doc for beautiful soup
        browser_session.get(URL)
        hltv_results = BeautifulSoup(browser_session.page_source, "html.parser")
        # hltv_results = BeautifulSoup(results_page.text, "html.parser")
        results = []
        # Pull match link from html
        for a in hltv_results.find_all("a", class_="a-reset"):
            link = str(a.get("href"))

            if link.startswith("/matches/"):
                results.append(link)
        return results

    def get_results(self, num_of_results: int = 10):
        browser_session = self.driver # Create browser session
        browser_session.get(self.base_url)
        print(f"did it work: {browser_session.title}")
        # Generate list of offset values, 0 always included as this is the final results page
        offset_values = [0]
        while self.results_page_offset > 0:
            offset_values.append(self.results_page_offset)
            # Use 100 as this is equivalent to going to the next page on hltv results
            self.results_page_offset -= 100

        # Get match URLS, ~100 matches per page offset decreases by 100 until most recent page ie. offset=100
        match_urls = self.scrape_results(
            offset=self.results_page_offset,
            browser_session=browser_session
        )
            # remove duplicate matches
        match_urls = list(set(match_urls))

        browser_session.close()
        return match_urls[:num_of_results]

    def scrape_match(self, match_url: str):
        browser_session = self.driver # Sets it to own var as calls to self.driver will keep creating new instances
        match_id = re.search(r"\d+", match_url).group()
        browser_session.get(f"{self.base_url}{match_url}")
    
        # Wait for page to load if not fully loaded
        time.sleep(5)

        match_details = BeautifulSoup(browser_session.page_source, "html.parser")
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
            demo_link = None

        try:
            # get demo id
            demo_a_tag = match_details.find("a", {"data-demo-link": True})

            demo_link = re.findall("\d+", demo_a_tag["data-demo-link"])[0]
            
        except:
            demo_link = None

        browser_session.close()

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

    def download_demos(self, demo_id: int, outdir: str | Path  = '.') -> None:
        demo_url = f"{self.base_url}/download/demo/{demo_id}"
        start_dir = os.getcwd()
        os.chdir(outdir)
        browser_session = self.driver

        browser_session.get(demo_url)

        files = os.listdir(os.curdir)
        
        # Wait for rar file to finish downloading 
        while not any(file.endswith('.rar') for file in files):
            # Refresh files var
            files = os.listdir(os.curdir)
            time.sleep(1)

        browser_session.close()
        os.chdir(start_dir)

        # returning working dir to use for parser after
        # TODO: Not sure if this is the 'dagster' way thinking that should be able to access the base_dir of I/O
