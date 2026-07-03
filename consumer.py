
from kafka_managers.consumer_handler import ConsumerHandler
import threading
from kafka_managers.consumers import run_consumer
import json

from topics import WIKIMEDIA_RECENT_TOPIC

class WikiMediaRecentChangesConsumer(ConsumerHandler):

    def consume(self):
        """Process the record. Runs BEFORE the offset is committed."""
        try:
            data = json.loads(self.value)
            print(f"Received change: data={data}, topic={self.topic}, partition={self.partition}, offset={self.offset}")
        except json.JSONDecodeError:
            print("Failed to decode JSON from message value.")
            

if __name__ == "__main__":
    threads = [
        threading.Thread(target=run_consumer, args=("wikimedia.recentchange.group_1", "wikimedia.recentchange.group_1.consumer_1", WIKIMEDIA_RECENT_TOPIC,WikiMediaRecentChangesConsumer)),
        threading.Thread(target=run_consumer, args=("wikimedia.recentchange.group_1", "wikimedia.recentchange.group_1.consumer_2", WIKIMEDIA_RECENT_TOPIC,WikiMediaRecentChangesConsumer)),
        threading.Thread(target=run_consumer, args=("wikimedia.recentchange.group_1", "wikimedia.recentchange.group_1.consumer_3", WIKIMEDIA_RECENT_TOPIC,WikiMediaRecentChangesConsumer)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

