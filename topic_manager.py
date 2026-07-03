from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "first_topic"


def create_topic(topic_name: str, num_partitions: int = 3, replication_factor: int = 1):
    # CLI equivalent:
    # kafka-topics --bootstrap-server localhost:9092 --create \
    #   --topic <topic_name> --partitions 3 --replication-factor 1
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )

    try:
        admin_client.create_topics(new_topics=[topic])
        print(f"Created topic '{topic_name}'.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    finally:
        admin_client.close()


def list_topics():
    # CLI equivalent:
    # kafka-topics --bootstrap-server localhost:9092 --list
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    try:
        topics = admin_client.list_topics()
        print("Topics:")
        for topic in sorted(topics):
            print(f"  - {topic}")
    finally:
        admin_client.close()


def describe_topic(topic_name: str):
    # CLI equivalent:
    # kafka-topics --bootstrap-server localhost:9092 --describe --topic <topic_name>
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    try:
        # describe_topics returns a list of dicts, one per requested topic
        details = admin_client.describe_topics([topic_name])
        for topic in details:
            for partition in topic["partitions"]:
                print(partition)
    finally:
        admin_client.close()


def delete_topic(topic_name: str):
    # CLI equivalent:
    # kafka-topics --bootstrap-server localhost:9092 --delete --topic <topic_name>
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    try:
        admin_client.delete_topics(topics=[topic_name])
        print(f"Deleted topic '{topic_name}'.")
    except UnknownTopicOrPartitionError:
        print(f"Topic '{topic_name}' does not exist.")
    finally:
        admin_client.close()