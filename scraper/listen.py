import pika
import scraper
import argparse

parser = argparse.ArgumentParser(description='Worker to scrape HLTV match pages, made to be deployed multiple times')

parser.add_argument('--work_number', 
                    type=int, 
                    default=1
                    help="Set worker number for debugging")

args = parser.parse_args()
work_num = args.work_number
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

channel.queue_declare(queue='match_queue')
print(f' [*] Worker {work_num} waiting for messages')

def callback(ch, method, properties, body):
    print(f' [x] Worker {work_num} recieved {body.decode()}')
    # Get match data from message
    data = body.decode()

    message = scraper.get_match_details(data)

    