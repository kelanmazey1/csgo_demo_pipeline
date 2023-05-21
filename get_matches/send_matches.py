import pika, json, argparse
import get_matches


def send_matches(args, ch):
    for match_url in get_matches.main(args):
        ch.basic_publish(exchange="", routing_key="my_test", body=match_url)
        print(f" [x] {match_url} added to queue")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrapes HLTV.org and loads results to a postgres DB"
    )
    parser.add_argument(
        "--offset",
        type=str,
        default=0,
        help="Number of matches to offset for HLTV results page, default is 0\nThis argument must be used with '=' ie. --offset=100",
    )

    parser.add_argument(
        "--date",
        type=str,
        help="Arg must be used with '=' ie. --date=\"25-12-2023\", no default",
    )

    args = parser.parse_args()
    # credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq", 5672))
    channel = connection.channel()
    channel.queue_declare(queue="my_test")

    send_matches(args, channel)

    connection.close()
