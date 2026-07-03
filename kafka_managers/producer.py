from functools import partial

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from kafka.serializer import DefaultSerializer

BOOTSTRAP_SERVERS = "localhost:9092"


# Dead-letter list: records that FAILED permanently (retries exhausted or a
# non-retriable error) are collected here so the caller can inspect or reprocess
# them instead of losing them to a print. Each entry is (topic, key, value,
# exception). In-memory only -> a real system would push these to a DLQ topic or
# a database; this is enough to see and recover failures in the course project.
dead_letters = []


# Callbacks run asynchronously on the producer's internal I/O thread once the
# broker replies -> keep them lightweight (no blocking work inside them).
def on_success(metadata):
    # called when the record is acknowledged by the broker
    print(
        f"Sent -> topic: {metadata.topic}"
        f"  partition: {metadata.partition}"
        f"  offset: {metadata.offset}"
    )


# The errback natively receives ONLY the exception, so it cannot tell which
# record failed. We bind the record via functools.partial in produce() ->
# partial(on_error, topic, key, value) leaves `exc` as the last arg the client
# fills in when the send fails.
def on_error(topic, key, value, exc):
    # called when the send ultimately fails (retries exhausted or non-retriable).
    print(f"Send failed -> topic: {topic}  key: {key}  error: {exc}")
    dead_letters.append((topic, key, value, exc))


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
            # compression_type: the producer compresses each BATCH of records
            # before sending. The codec is written into the batch header, so the
            # broker stores it compressed and the CONSUMER decompresses it
            # automatically -> no manual decompression on the consumer side.
            # "zstd" gives ~gzip compression ratio at much higher speed; it needs
            # the `zstandard` package installed on BOTH producer and consumer,
            # and brokers >= 2.1. Our wikimedia JSON is very repetitive so it
            # compresses a lot. Bigger batches (linger_ms / batch_size) compress
            # better.
            compression_type="zstd",
            # linger_ms: how long the producer waits for MORE records to join a
            # batch before sending it, even if the batch is not full. The default
            # is 0 -> a batch is sent the instant the I/O thread is free, so under
            # low load we often ship batches of 1. Waiting ~20ms lets records
            # accumulate into fuller batches, which improves throughput and (with
            # zstd above) compression, at the cost of a little latency. Great
            # trade for a high-volume, very repetitive feed like wikimedia.
            linger_ms=20,
            # batch_size: the MAX size (in bytes) of a single batch, PER
            # partition. Once accumulated records for a partition reach this many
            # bytes the batch is sent immediately. Default is 16384 (16 KB); we
            # double it to 32 KB so batches hold more records -> better
            # compression and fewer requests, using a bit more memory per
            # partition. It is a cap, not a target: a batch can still be sent
            # early when linger_ms elapses. A record larger than batch_size is not
            # batched at all. A batch flushes when EITHER limit is hit first:
            # batch_size bytes reached OR linger_ms elapsed.
            batch_size=32 * 1024,
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
    # send() is async, but it can still raise SYNCHRONOUSLY on THIS thread before
    # any future exists -> e.g. KafkaTimeoutError when the buffer_memory is full
    # and max_block_ms elapsed (producing faster than the broker drains), a
    # serialization error, or the producer being already closed. Wrap it so a
    # producer-side failure is captured as a dead letter instead of crashing the
    # caller.
    try:
        # register callbacks that fire later, on the producer's I/O thread, when
        # the broker responds (or the send ultimately fails). on_error is bound
        # to this record via partial so the errback knows WHICH message failed.
        get_producer().send(topic_name, key=key, value=value) \
            .add_callback(on_success) \
            .add_errback(partial(on_error, topic_name, key, value))
    except KafkaTimeoutError as exc:
        # buffer full -> back-pressure signal. Don't drop silently; record it.
        print(f"Buffer full, send blocked -> key: {key}  error: {exc}")
        dead_letters.append((topic_name, key, value, exc))
    except KafkaError as exc:
        # serialization error, producer closed, etc. -> caller's thread.
        print(f"send() failed synchronously -> key: {key}  error: {exc}")
        dead_letters.append((topic_name, key, value, exc))


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
