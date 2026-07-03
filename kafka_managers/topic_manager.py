from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

BOOTSTRAP_SERVERS = "localhost:9092"


def create_topic(
    topic_name: str,
    num_partitions: int = 3,
    replication_factor: int = 1,
    min_insync_replicas: int = 1,
):
    # min_insync_replicas: the minimum number of replicas (leader + followers)
    #   that must acknowledge a write for it to be considered successful.
    #   This is a TOPIC-level config, not a producer arg. It ONLY takes effect
    #   when the producer sends with acks="all": in that case a write succeeds
    #   only once at least `min_insync_replicas` replicas have it, otherwise the
    #   broker rejects the write (NotEnoughReplicasException). With acks=0 or
    #   acks=1 this setting is ignored.
    #   Typical durable setup: replication_factor=3 + min_insync_replicas=2,
    #   which tolerates the loss of one replica while still guaranteeing writes.
    #
    # CLI equivalent:
    # kafka-topics --bootstrap-server localhost:9092 --create \
    #   --topic <topic_name> --partitions 3 --replication-factor 1 \
    #   --config min.insync.replicas=1
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        topic_configs={"min.insync.replicas": str(min_insync_replicas)},
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