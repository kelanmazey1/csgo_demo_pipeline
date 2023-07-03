import pika, patool, requests, os, urllib, shutil


def main(match_details: str):
    os.chdir("/demo_dump")
    print("changed dir")
    # demo_url = f'https://www.hltv.org/download/demo/{match_details['demo_id']}'
    demo_url = "https://www.hltv.org/download/demo/79831"

    req = urllib.request.Request(
        demo_url,
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Brave";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "sec-gpc": "1",
            "upgrade-insecure-requests": "1",
        },
    )

    with urllib.request.urlopen(req) as response, open(
        "./rar_files/demo.rar", "wb"
    ) as out_file:
        shutil.copyfileobj(response, out_file)

    patool.extract_archive("./rar_files/demo.rar", outdir="./demo_files/")


if __name__ == "__main__":
    main("hello")
