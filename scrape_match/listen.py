import pika
import scrape_match
import argparse

parser = argparse.ArgumentParser(
    description="Worker to scrape HLTV match pages, made to be deployed multiple times"
)

parser.add_argument(
    "--work_number", type=int, default=1, help="Set worker number for debugging"
)

args = parser.parse_args()
work_num = args.work_number
connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq", 5672))
channel = connection.channel()

channel.queue_declare(queue="my_test")


def callback(ch, method, properties, body):
    print(f" [x] Worker {work_num} recieved {body}")
    # Get match data from message, cast string from byte message
    data_blob = scrape_match.get_match_details(body.decode())

    print(f" [x] Worker {work_num} processed blob: \n {data_blob}")

    # message = scraper.get_match_details(data)


channel.basic_consume(queue="my_test", auto_ack=True, on_message_callback=callback)
print(f" [*] Worker {work_num} waiting for messages")
channel.start_consuming()
