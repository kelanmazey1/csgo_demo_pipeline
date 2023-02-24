import requests
from bs4 import BeautifulSoup


def main():
  # Get HTML doc for beautiful soup
  # Hardcoding initial offset as this is as far back as matches go, getting future demos can be from daily run of "hltv.org/results"
  # Do I want to add an offset of 1 to wait for demos?
  urls = get_urls()

  # TODO: Get list of HLTV match IDs + team names + competition

  # TODO: Check URL to see if demo

  # TODO: Save data: HLTV_match_URL, HLTV_match_id, team_a, team_b, competition, demo_available, date


def get_urls(**kwargs):
  offset = kwargs.get("offset", 0)

  results_page = requests.get(
    url=f"https://www.hltv.org/results/offset={offset}",
    headers={
      "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
      "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    })
  
  hltv_results = BeautifulSoup(results_page.text, 'html.parser')
  results =[]

  # Pull match link from html
  for a in hltv_results.find_all("a", class_="a-reset"):
    link = str(a.get("href"))
    
    if link.startswith("/matches/"):
      results.append(link)

if __name__ == "__main__":

  main()