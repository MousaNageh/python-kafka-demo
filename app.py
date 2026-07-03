from topic_manager import create_topic 
from producer import produce
import json
topic_name = "first_topic"
# create_topic(topic_name=topic_name, num_partitions=3, replication_factor=1)
user_id = 1
value = {"user_id": user_id, "name": "mousa", "age": 30, "city": "New York"}
while user_id < 10:    
    produce(topic_name=topic_name, key="user_" + str(value["user_id"]), value=json.dumps(value), acks="all")
    user_id += 1
    value["user_id"] = user_id
