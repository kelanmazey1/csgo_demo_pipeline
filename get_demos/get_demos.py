import re
import requests
import argparse
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, List, Tuple
import undetected_chromedriver as uc

# Create web driver - uses undected to avoid cloudflare
options = uc.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
driver = uc.Chrome(options=options)
HLTV_ADDR = 'https://www.hltv.org'

driver.get(HLTV_ADDR)
print(f'did it work: {driver.title}')

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
  print('test')
  for offset in offset_values:
    match_urls += (get_match_urls(offset=offset))
    # # remove duplicate matches
    match_urls = list(set(match_urls))
    # # Go to match page and parse data
    data_to_load = get_match_details(match_urls)
    # Send demo link + metadata to event bridge for processing
    parquet_dump(data_to_load)

  driver.close()

def get_match_urls(offset: int) -> List[str]:
  # Get HTML doc for beautiful soup
  # Uses requests instead of chrome driver for sake of speed
  results_page = requests.get(f'{HLTV_ADDR}/results?offset={offset}')
  
  hltv_results = BeautifulSoup(results_page.text, 'html.parser')
  results = []

  # Pull match link from html
  for a in hltv_results.find_all("a", class_="a-reset"):
    link = str(a.get("href"))
    
    if link.startswith("/matches/"):
      results.append(link)
  return results

def get_match_details(match_urls: List[str]) -> List[Dict[int, Dict[str, str]]]:
  match_details_list = []
  url = '/matches/2363184/astralis-vs-spirit-blasttv-paris-major-2023-europe-rmr-b'
  # TODO: Add try catch and handle if info isn't available on the page
  # for url in match_urls:  
    
  # get match_id from url
  match_id = re.search(r"\d+", url).group()
  driver.get(f'{HLTV_ADDR}{url}')
  match_details = BeautifulSoup(driver.page_source, 'html.parser')
  team_box = match_details.find('div', class_='standard-box teamsBox')
  # Get team
  team_a_div = team_box.find('div', class_='team1-gradient')
  team_a = team_a_div.find('div').text
  team_b_div = team_box.find('div', class_='team2-gradient')
  team_b = team_b_div.find('div').text
  # 2 divs with name so de-duping
  team_a_score = team_a_div.find('div', class_=['won', 'lost']).text
  team_b_score = team_b_div.find('div', class_=['won', 'lost']).text
  # get competition from page
  competition_div = team_box.find('div', class_= 'event text-ellipsis')
  competition = competition_div.find('a').text
  # get date and cast from page
  unix_datetime_div = team_box.find('div', {'class': 'time','data-unix': True})
  unix_datetime = int(unix_datetime_div['data-unix'])

  # get demo id
  demo_a_tag = match_details.find('a', {'data-demo-link': True})
  demo_link = re.findall('\d+', demo_a_tag['data-demo-link'])[0]
  
  data_dict = {
    'hltv_id': int(match_id),
    'url': url,
    'team_a': team_a,
    'team_b': team_b,
    'team_a_score': int(team_a_score),
    'team_b_score': int(team_b_score),
    'competition': competition,
    'date': unix_datetime,
    'demo_id': int(demo_link),
  }
  match_details_list.append(data_dict)
  # print(match_details_list)
  return match_details_list

def parquet_dump(data, **kwargs):
  #set schema
  schema = pa.schema([
    pa.field('match_id', pa.int32()),
    pa.field('url', pa.string()),
    pa.field('team_a', pa.string()),
    pa.field('team_b', pa.string()),
    pa.field('team_a_score', pa.int8()),
    pa.field('team_b_score', pa.int8()),
    pa.field('competition', pa.string()),
    pa.field('date', pa.date64()),
    pa.field('demo_id', pa.int32()),
  ])
  
  #create pyarrow table
  table = pa.Table.from_pylist(data, schema=schema)
  print(table)

  path = (args.bucket if args.bucket else '/demo_dump/process/') + 'match_details.parquet'
  print(path)
  #save pyarrow table to parquet
  pq.write_table(table, path)
  return

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Scrapes HLTV.org and loads results to a postgres DB")
  parser.add_argument("--offset", type=str, default=0, help="Number of matches to offset for HLTV results page, default is 0\nThis argument must be used with '=' ie. --offset=100")

  parser.add_argument("--bucket", type=str, help="S3 bucket can be passed as destination to save match details parquet, otherwise will save to current directory")

  args = parser.parse_args()
  main(args)