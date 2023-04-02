import requests
import argparse
from bs4 import BeautifulSoup


def main(args):
  # Get match URLS
  match_urls = get_match_urls(args)

  # TODO: Check URL to see if demo
  check_demos = get_demo_urls(match_urls)

  # TODO: Save data: HLTV_match_URL, HLTV_match_id, team_a, team_b, competition, demo_url, date
  # load_data_to_db()

def get_match_urls(args) -> list[str]:
  # Get HTML doc for beautiful soup
  offset = args.offset
  results_page = requests.get(
    url=f"https://www.hltv.org/results?offset={offset}",
    headers={
      "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
      "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    })
  
  hltv_results = BeautifulSoup(results_page.text, 'html.parser')
  raw_results =[]

  # Pull match link from html
  for a in hltv_results.find_all("a", class_="a-reset"):
    link = str(a.get("href"))
    
    if link.startswith("/matches/"):
      raw_results.append(link)
  
  # This is to remove any duplicates added from the 'Featured results' pane on most recent matches
  # NOTE: I tried to use bs4 to search for the results-all class but couldn't get it to return  
  results = []
  for match in raw_results:
    if match not in results:
      results.append(match)

  return results

def get_demo_urls(match_urls: list[str]) -> tuple[str, str, str]:
  pass

def load_data_to_db(db_connection, **kwargs):
  pass

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Scrapes HLTV.org and loads results to a postgres DB")
  parser.add_argument("--offset", type=str, default=0, help="Number of matches to offset for HLTV results page, default is 0\nThis argument must be used with '=' ie. --offset=100")

  parser.add_argument("--date", type=str, help="Arg must be used with '=' ie. --date=\"25-12-2023\", no default")

  args = parser.parse_args()
  main(args)