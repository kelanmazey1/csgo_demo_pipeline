import requests
from bs4 import BeautifulSoup


def main():
  # TODO: Get HTML doc for beautiful soup
  # Hardcoding initial offset as this is as far back as matches go, getting future demos can be from daily run of "hltv.org/results"
  # Do I want to add an offset of 1 to wait for demos?
  results_page = requests.get(
    url="https://www.hltv.org/results",
    headers={
      "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
      "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    })
  print(type(results_page.text))
  
  # TODO: Get list of HLTV match IDs + team names + competition
  # TODO: 

  # TODO: Create URL to scrape for each match

  # TODO: Check URL to see if demo

  # TODO: Save data: HLTV_match_URL, HLTV_match_id, team_a, team_b, competition, demo_available, date


if __name__ == "__main__":
  main()