import patoolib, requests, os, urllib, shutil, sys, time
import rarfile
from typing import Dict
import requests
import undetected_chromedriver as uc

def dl_unzip(match_details: Dict):
    # demo_url = f"https://www.hltv.org/download/demo/{match_details['demo_id']}"
    demo_url = "https://www.hltv.org/download/demo/79831"
    DEMO_STORE = "./demo_files"

    dir_name = f"{match_details['team_a']}-vs-{match_details['team_b']}-{match_details['date']}"

    os.mkdir(dir_name)

    work_dir_abs = os.path.abspath(dir_name)

    os.chdir(work_dir_abs)
    os.mkdir("./demo_files")
    
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    driver = uc.Chrome(options=options)

    driver.get(demo_url)

    # Initially check if archive downloaded if not sleep for 10 seconds
    files = os.listdir(os.curdir)

    # Wait for rar file to finish downloading 
    while not any(file.endswith('.rar') for file in files):
        # Refresh files var
        files = os.listdir(os.curdir)
        time.sleep(1)
    
    demo_archive = [f for f in os.listdir(os.curdir) if f.endswith('.rar')]

    if len(demo_archive) > 1:
        raise FileExistsError(f"More than one archive found: {demo_archive}")

    patoolib.extract_archive(demo_archive[0], outdir=DEMO_STORE)

    driver.close()

    # If number of files doesn't add up to maps played raise warning
    if match_details['maps_played'] != len(os.listdir(DEMO_STORE)):
        raise UserWarning("Number of demos doesn't match number of matches played!")
    

    # Perform clean up
    for file in os.listdir(work_dir_abs):
        if file.endswith(".rar") or file == "chromedriver":
            path = os.path.join(work_dir_abs, file)
            os.remove(path)
    

    print(os.path.join(work_dir_abs, DEMO_STORE))
    # return demo_files location
    return os.path.join(work_dir_abs, DEMO_STORE)

if __name__ == "__main__":
    dl_unzip({
        "team_a": "test1",
        "team_b": "test2",
        "date": "2023-07-08",
        "maps_played": 3, 
    })
