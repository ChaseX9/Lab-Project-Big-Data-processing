import re
from confluent_kafka import Consumer

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book-group',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)
consumer.subscribe(['book'])

MAX_EMPTY_POLLS = 10
MAX_ERRORS = 5
empty_polls = 0
error_count = 0

with open('output.txt', 'w', encoding='utf-8') as out:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Closing: No new messages received.")
                break
            continue

        if msg.error():
            error_count += 1
            print(f"Consumer error: {msg.error()}")
            if error_count >= MAX_ERRORS:
                print("Closing: Too many consecutive errors.")
                break
            continue

        empty_polls = 0
        error_count = 0

        line = msg.value().decode('utf-8')
        line = line.lower()
        line = re.sub(r'[^\w\s]', '', line)
        line = ' '.join(line.split())

        if line:
            out.write(line + '\n')

consumer.close()
print("Terminé, résultat dans output.txt")
