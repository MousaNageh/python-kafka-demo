import threading

from kafka import KafkaConsumer
from kafka.serializer import DefaultSerializer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "first_topic"


def run_consumer(group_id: str, consumer_name: str):
    """Start one consumer that belongs to the group `group_id`.

    -------------------------------------------------------------------
    CONSUMER OFFSET
    -------------------------------------------------------------------
    An "offset" is the position of a record inside a partition (0, 1, 2 ...).
    Each consumer GROUP tracks, per partition, the offset of the NEXT record
    it should read. Kafka stores these committed offsets in an internal
    topic called "__consumer_offsets".

    - enable_auto_commit=False -> we commit MANUALLY. The offset is marked as
      consumed only AFTER we have fully processed the message (consumer.commit()).
      This is "at-least-once" delivery: if the consumer crashes after processing
      but before committing, the message is simply re-read on restart (never lost).

    -------------------------------------------------------------------
    DELIVERY SEMANTICS (decided by WHEN you commit the offset)
    -------------------------------------------------------------------
    AT-MOST-ONCE  -> commit the offset BEFORE processing the message.
        order: read -> commit offset -> process
        A crash after commit but before processing LOSES the message.
        No duplicates, but messages can be missed. (fast, least safe)
        In kafka-python: enable_auto_commit=True (auto-commits early).

    AT-LEAST-ONCE -> commit the offset AFTER processing the message.  <-- THIS CODE
        order: read -> process -> commit offset
        A crash after processing but before commit RE-READS the message,
        so it may be processed twice. Never lost, but can be duplicated.
        Make your processing idempotent to stay correct.

    EXACTLY-ONCE  -> each message takes effect once, no loss, no duplicates.
        Not achievable with plain offset commits. You need EITHER:
          * Kafka transactions (read-process-write fully inside Kafka:
            producer.init_transactions / send_offsets_to_transaction), OR
          * an idempotent/transactional sink where the processing result and
            the offset are written together atomically (e.g. same DB tx).
        Strongest guarantee, most overhead.
        
    Because offsets are tracked PER GROUP:
    - Consumers in the SAME group share the work: each partition is read by
      exactly one consumer in the group (load balancing).
    - Different groups are independent: every group receives ALL the messages.
    """
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        # auto_offset_reset -> what to do ONLY when the group has NO valid
        # committed offset (a brand-new group, or the saved offset expired).
        # If a committed offset already exists, this setting is IGNORED and the
        # consumer resumes from that offset instead.
        #   "earliest" -> start from the FIRST record in the partition (read all
        #                 history from the beginning).
        #   "latest"   -> start from the END; only read records produced AFTER
        #                 the consumer joins (this is the Kafka default).
        #   "none"     -> do NOT guess: raise an error if there is no committed
        #                 offset for the group (you must handle it yourself).
        auto_offset_reset="earliest",
        # turn OFF automatic committing -> we decide when the offset is marked
        enable_auto_commit=False,
        # DefaultSerializer also works as a Deserializer: utf-8 bytes -> str
        # (None passes through). Proper kafka.serializer type, no deprecation.
        key_deserializer=DefaultSerializer(),
        value_deserializer=DefaultSerializer(),
        # no consumer_timeout_ms -> this loop blocks and polls forever
    )

    print(f"[{group_id}/{consumer_name}] started, polling forever...")

    # never exits: keeps waiting for and consuming new messages
    for record in consumer:
        # 1) process the message
        print(
            f"[{group_id}/{consumer_name}] "
            f"partition={record.partition} offset={record.offset} "
            f"key={record.key} value={record.value}"
        )

        # 2) only NOW mark the offset as consumed (commit after processing).
        #    commit() with no args commits the offsets of the records already
        #    returned by poll(), i.e. up to and including this one.
        consumer.commit()


if __name__ == "__main__":
    threads = [
        # Group A: 2 consumers -> the 3 partitions of first_topic are split
        # between them (e.g. one reads 2 partitions, the other reads 1).
        threading.Thread(target=run_consumer, args=("group_A", "consumer_1")),
        threading.Thread(target=run_consumer, args=("group_A", "consumer_2")),
        # Group B: 1 consumer -> it alone reads ALL 3 partitions and receives
        # every message, independently of group_A.
        threading.Thread(target=run_consumer, args=("group_B", "consumer_1")),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()
