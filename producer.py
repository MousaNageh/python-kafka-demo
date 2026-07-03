from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer

BOOTSTRAP_SERVERS = "localhost:9092"


# Callbacks run asynchronously on the producer's internal I/O thread once the
# broker replies -> keep them lightweight (no blocking work inside them).
def on_success(metadata):
    # called when the record is acknowledged by the broker
    print(
        f"Sent -> topic: {metadata.topic}"
        f"  partition: {metadata.partition}"
        f"  offset: {metadata.offset}"
    )


def on_error(exc):
    # called when the send ultimately fails
    print(f"Send failed: {exc}")


def produce(topic_name, key, value, acks="all"):
    """Send one record to topic_name.

    key   : message key (str or None) -> decides the partition
    value : message value (str)
    acks  : 0    -> fire and forget (no broker acknowledgement)
            1    -> wait for the leader to write the record
            "all"-> wait for the leader + all in-sync replicas (safest)
    """
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks=acks,
        # DefaultSerializer: str -> utf-8 bytes (bytes/None pass through). It is a
        # proper kafka.serializer.Serializer, unlike the deprecated lambda form.
        key_serializer=DefaultSerializer(),
        value_serializer=DefaultSerializer(),
    )

    # send() is async and returns a future; instead of blocking with .get(),
    # register callbacks that fire later when the broker responds.
    producer.send(topic_name, key=key, value=value) \
        .add_callback(on_success) \
        .add_errback(on_error)

    # flush() forces buffered records to be delivered NOW; the callbacks above
    # fire during this call before we close the producer.
    producer.flush()
    producer.close()



# ---------------------------------------------------------------------------
# CLI equivalent using kafka-console-producer
# ---------------------------------------------------------------------------
#
# 1) Produce WITHOUT a key (value only). Type messages, one per line,
#    then press Ctrl+C / Ctrl+D to stop:
#
#    kafka-console-producer --bootstrap-server localhost:9092 \
#      --topic first_topic
#
# 2) Produce WITH a key/value (key and value separated by ":"):
#
#    kafka-console-producer --bootstrap-server localhost:9092 \
#      --topic first_topic \
#      --property parse.key=true \
#      --property key.separator=:
#    > user1:hello with acks=all
#    > user2:hello with acks=1
#
# 3) Control the acks (durability) level with --producer-property:
#
#    # acks=all  -> leader + all in-sync replicas
#    kafka-console-producer --bootstrap-server localhost:9092 \
#      --topic first_topic --producer-property acks=all
#
#    # acks=1    -> leader only
#    kafka-console-producer --bootstrap-server localhost:9092 \
#      --topic first_topic --producer-property acks=1
#
#    # acks=0    -> fire and forget
#    kafka-console-producer --bootstrap-server localhost:9092 \
#      --topic first_topic --producer-property acks=0
#
# 4) Everything combined (key/value + acks=all):
#
#    kafka-console-producer --bootstrap-server localhost:9092 \
#      --topic first_topic \
#      --property parse.key=true --property key.separator=: \
#      --producer-property acks=all
