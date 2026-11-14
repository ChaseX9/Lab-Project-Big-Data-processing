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
import sys
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
MAX_RETRY_DELAY = 60  # seconds


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


def events(deadline, last_event_id=None):
    """Yield ``(event_id, event)`` from the stream until ``deadline``.

    ``deadline`` is a ``time.monotonic()`` value. On errors (network, HTTP
    403/429/5xx) it reconnects, waiting 5 s, then twice as long each time up
    to 60 s; the deadline is also checked there, so the producer stops on
    time even when the stream never answers.
    """
    delay = 5
    while time.monotonic() < deadline:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        try:
            with requests.get(STREAM_URL, headers=headers, stream=True,
                              timeout=(10, 60)) as response:
                response.raise_for_status()
                delay = 5
                event_id, data = None, []
                for raw in response.iter_lines(decode_unicode=True):
                    if time.monotonic() >= deadline:
                        return
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
            wait = min(delay, max(0, deadline - time.monotonic()))
            print(f"stream error: {error}, reconnecting in {wait:.0f} s")
            time.sleep(wait)
            delay = min(delay * 2, MAX_RETRY_DELAY)


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
    failed = []

    def on_delivery(error, message):
        # Called by poll() / flush() once Kafka has accepted or refused it.
        if error is not None:
            failed.append(error)

    deadline = time.monotonic() + args.minutes * 60
    end = datetime.now() + timedelta(minutes=args.minutes)
    sent = 0
    print(f"Producing {args.wiki} changes to '{args.topic}' until {end:%H:%M:%S}")
    for _, event in events(deadline):
        if event.get("wiki") != args.wiki:
            continue
        producer.produce(args.topic, key=event.get("title", "").encode("utf-8"),
                         value=json.dumps(event).encode("utf-8"),
                         on_delivery=on_delivery)
        producer.poll(0)
        sent += 1
        if sent % 1000 == 0:
            print(f"{datetime.now():%H:%M:%S} {sent:,} events sent")
    not_flushed = producer.flush(30)
    delivered = sent - len(failed) - not_flushed
    print(f"Done: {sent:,} events sent, {delivered:,} delivered to Kafka")
    if failed or not_flushed:
        print(f"WARNING: {len(failed):,} delivery errors"
              + (f" (first: {failed[0]})" if failed else "")
              + f", {not_flushed:,} still waiting after the 30 s flush")
        sys.exit(1)


if __name__ == "__main__":
    main()
