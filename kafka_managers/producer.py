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


# One shared producer for the whole process. Idempotence guarantees (dedup of
# retries + ordering) are scoped to a single producer session, so the producer
# MUST be long-lived and reused across messages -> recreating it per message
# would restart the producer id/sequence and throw those guarantees away. It is
# also expensive (new connections + metadata fetch) to build one per record.
_producer = None


def get_producer():
    """Return the process-wide idempotent producer, creating it on first use."""
    global _producer
    if _producer is None:
        # enable_idempotence=True makes the producer exactly-once *per
        # partition*: the broker de-duplicates retried records (via a producer
        # id + sequence number), so a retry after a flaky ack never writes a
        # duplicate and order is preserved. It REQUIRES acks="all", retries > 0,
        # and max_in_flight_requests_per_connection <= 5 -> the kafka-python
        # client sets those safe defaults for us, but we force acks="all" so
        # idempotence is not silently disabled.
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks="all",
            enable_idempotence=True,
            # DefaultSerializer: str -> utf-8 bytes (bytes/None pass through). It
            # is a proper kafka.serializer.Serializer, unlike the deprecated
            # lambda form.
            key_serializer=DefaultSerializer(),
            value_serializer=DefaultSerializer(),
        )

        # KafkaProducer merges our overrides with all its defaults into
        # _producer.config -> print the full resolved config so we can see the
        # effective values (acks, retries, max_in_flight_requests_per_connection,
        # enable_idempotence, ...) the client actually chose.
        print("Producer config:")
        for name, value in _producer.config.items():
            print(f"  {name} = {value}")
    return _producer


def produce(topic_name, key, value):
    """Queue one record for topic_name on the shared idempotent producer.

    key   : message key (str or None) -> decides the partition
    value : message value (str)

    This producer is idempotent, which forces acks="all". With acks="all" the
    topic's min.insync.replicas (set in create_topic) applies: the write is
    rejected unless at least that many replicas have the record.

    send() is async and buffered -> it does NOT block or flush here. Call
    flush_producer() to force delivery and close_producer() on shutdown.
    """
    # register callbacks that fire later, on the producer's I/O thread, when the
    # broker responds (or the send ultimately fails).
    get_producer().send(topic_name, key=key, value=value) \
        .add_callback(on_success) \
        .add_errback(on_error)


def flush_producer():
    """Block until all buffered records are delivered and callbacks have run."""
    if _producer is not None:
        _producer.flush()


def close_producer():
    """Flush and close the shared producer. Call once on process shutdown."""
    global _producer
    if _producer is not None:
        _producer.flush()
        _producer.close()
        _producer = None



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
