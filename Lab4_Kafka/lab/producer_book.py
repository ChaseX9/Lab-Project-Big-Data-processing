import socket
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

topic = 'book'

with open('book.txt', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            producer.produce(topic=topic, value=line)
            producer.poll(0)

producer.flush()
print("Livre envoyé")
