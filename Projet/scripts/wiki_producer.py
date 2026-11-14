"""Send live Wikipedia change events to Kafka.

Reads the Wikimedia EventStreams ``recentchange`` feed (server-sent events),
keeps the events of one wiki (English Wikipedia by default) and produces
them, unchanged, as JSON to a Kafka topic. Filtering on IMDb relevance is
done later by the Spark streaming job.

Usage (normally through Docker Compose: ``docker compose run --rm producer``):

    python scripts/wiki_producer.py --minutes 30
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = (
    "ECE-BigData-IMDb-Project/1.0 "
    "(student project; https://github.com/ChaseX9/Lab-Project-Big-Data-processing)"
)


def create_topic(bootstrap, topic, partitions=1):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, partitions, 1)])
    for future in futures.values():
        try:
            future.result()
        except Exception as error:  # topic created concurrently
            print(f"topic creation: {error}")


def events(last_event_id=None):
    """Yield ``(event_id, event)`` from the stream, reconnecting on errors."""
    while True:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        try:
            with requests.get(STREAM_URL, headers=headers, stream=True,
                              timeout=(10, 60)) as response:
                response.raise_for_status()
                event_id, data = None, []
                for raw in response.iter_lines(decode_unicode=True):
                    if raw is None:
                        continue
                    if raw.startswith("id:"):
                        event_id = raw[3:].strip()
                    elif raw.startswith("data:"):
                        data.append(raw[5:].strip())
                    elif raw == "" and data:
                        try:
                            yield event_id, json.loads("".join(data))
                        except json.JSONDecodeError:
                            pass
                        last_event_id = event_id or last_event_id
                        event_id, data = None, []
        except requests.RequestException as error:
            print(f"stream error: {error}, reconnecting in 5 s")
            time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--minutes", type=float, default=30)
    parser.add_argument("--wiki", default="enwiki")
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "wikipedia-changes"))
    parser.add_argument("--bootstrap",
                        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    args = parser.parse_args()

    create_topic(args.bootstrap, args.topic)
    producer = Producer({"bootstrap.servers": args.bootstrap, "compression.type": "lz4",
                         "linger.ms": 100})
    stop = datetime.now() + timedelta(minutes=args.minutes)
    sent = 0
    print(f"Producing {args.wiki} changes to '{args.topic}' until {stop:%H:%M:%S}")
    for _, event in events():
        if datetime.now() >= stop:
            break
        if event.get("wiki") != args.wiki:
            continue
        producer.produce(args.topic, key=event.get("title", "").encode("utf-8"),
                         value=json.dumps(event).encode("utf-8"))
        producer.poll(0)
        sent += 1
        if sent % 1000 == 0:
            print(f"{datetime.now():%H:%M:%S} {sent:,} events sent")
    producer.flush(30)
    print(f"Done: {sent:,} events sent")


if __name__ == "__main__":
    main()
