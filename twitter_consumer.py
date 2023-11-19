from kafka import KafkaConsumer

from json import loads
from rich import print

consumer = KafkaConsumer(
    'my-test-topic',
    bootstrap_servers=['vps-data1:9092','vps-data2:9092','vps-data3:9092'],
    auto_offset_reset = 'latest',
    enable_auto_commit = True,
    group_id = 'None',
    value_deserializer = lambda x : loads(x.decode('utf-8'))
)

for msg in consumer:
    tweet = msg.value
    print(tweet)