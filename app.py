import requests
from kafka_managers.producer import produce, close_producer, dlq_topic_for
from kafka_managers.topic_manager import create_topic
from topics import WIKIMEDIA_RECENT_TOPIC
import json

create_topic(WIKIMEDIA_RECENT_TOPIC, num_partitions=3, replication_factor=1)
# create the dead-letter topic the producer routes failed sends to
# (dlq_topic_for -> "wikimedia.recentchange.dlq"), so it exists up front instead
# of relying on broker auto-creation.
create_topic(dlq_topic_for(WIKIMEDIA_RECENT_TOPIC), num_partitions=3, replication_factor=1)

url = "https://stream.wikimedia.org/v2/stream/recentchange"
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
headers = {"User-Agent": user_agent}
res = requests.get(url, stream=True, headers=headers)
for line in res.iter_lines():
    if line:
        line = line.decode("utf-8").strip(" data:  ")
        try:
            data = json.loads(line)
            produce(topic_name=WIKIMEDIA_RECENT_TOPIC, key=None, value=json.dumps(data))
        except json.JSONDecodeError:
            pass 

close_producer()