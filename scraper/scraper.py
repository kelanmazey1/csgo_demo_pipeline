import re
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd 
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

def get_match_details(match_urls: List[str]) -> List[Dict[int, Dict[str, str]]]:
  data_dict = {}
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
  unix_datetime = int(unix_datetime_div['data-unix']) / 1000
  # Convert to human readable
  # TODO: Currently converts date time to 1 hour out, I think???
  date_time = datetime.utcfromtimestamp(unix_datetime).strftime('%Y-%m-%dT%H:%M:%SZ')
  # get data demo ID 
  demo_a_tag = match_details.find('a', {'data-demo-link': True})
  demo_link = demo_a_tag['data-demo-link']
  
  data_dict[match_id] = {
    'url': url,
    'team_a': team_a,
    'team_b': team_b,
    'team_a_score': team_a_score,
    'team_b_score': team_b_score,
    'competition': competition,
    'date': date_time,
    'demo_id': demo_link,
  }
  return data_dict

if __name__ == "__main__":
  main()