import time
from typing import Optional, Type

from kafka import KafkaConsumer, TopicPartition
from kafka.serializer import DefaultSerializer

from kafka_managers.consumer_handler import ConsumerHandler, PoisonMessage
from kafka_managers.producer import produce, flush_producer


# how long to wait before re-reading a record that hit a TRANSIENT error, so we
# don't hammer a sink that is momentarily down.
RETRY_BACKOFF_SECONDS = 3

# how many times to retry a record on a TRANSIENT error before giving up on it
# and routing it to the dead-letter topic. Constant, per record.
MAX_RETRIES = 5

# bulk consumer knobs.
# BULK_MAX_RECORDS -> most records pulled per poll() and sent in one bulk request.
#   Bigger = fewer round-trips/higher throughput, but more memory per batch and
#   more work to redo if the batch is replayed after a crash.
# BULK_POLL_TIMEOUT_MS -> how long poll() waits to fill a batch before returning
#   what it has, so a low-traffic stream still flushes promptly.
BULK_MAX_RECORDS = 500
BULK_POLL_TIMEOUT_MS = 1000


def _bulk_error_is_transient(status: int) -> bool:
    """Classify a per-document bulk failure by HTTP status:
    429 (too many requests) or any 5xx -> TRANSIENT (retry the doc);
    everything else (e.g. 400 mapping/parse error) -> PERMANENT (dead-letter)."""
    return status == 429 or status >= 500


def _send_to_dead_letter(dead_letter_topic: str, record, exc: Exception):
    """Park a record that failed all retries on the dead-letter topic, then
    FLUSH so it is durably stored BEFORE the caller commits past the original.

    We forward the original key/value unchanged and stash the failure reason and
    the origin (topic-partition-offset) in headers, so the DLQ is inspectable and
    the record can be replayed later. Flushing here is essential: if we committed
    the original offset before the DLQ write landed and then crashed, the record
    would be lost from BOTH topics.
    """
    headers = [
        # dlq-source lets a DLQ reader tell WHICH side failed: "consumer" here vs
        # "producer" for records the producer itself could not send.
        ("dlq-source", b"consumer"),
        ("dlq-error", str(exc).encode("utf-8")),
        ("dlq-origin", f"{record.topic}-{record.partition}@{record.offset}".encode("utf-8")),
    ]
    produce(dead_letter_topic, key=record.key, value=record.value, headers=headers)
    flush_producer()


BOOTSTRAP_SERVERS = "localhost:9092"


def run_consumer(
    group_id: str,
    consumer_name: str,
    topic_name: str,
    handler_cls: Type[ConsumerHandler],
    auto_offset_reset="earliest",
    dead_letter_topic: Optional[str] = None,
):
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
        topic_name,
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
        auto_offset_reset=auto_offset_reset,
        # turn OFF automatic committing -> we decide when the offset is marked
        enable_auto_commit=False,
        # DefaultSerializer also works as a Deserializer: utf-8 bytes -> str
        # (None passes through). Proper kafka.serializer type, no deprecation.
        key_deserializer=DefaultSerializer(),
        value_deserializer=DefaultSerializer(),
        # no consumer_timeout_ms -> this loop blocks and polls forever
    )

    print(
        f"[{group_id}/{consumer_name}] started with {handler_cls.__name__}, "
        f"polling forever..."
    )

    # -------------------------------------------------------------------
    # COMMIT STRATEGIES (HOW you commit the offset)
    # -------------------------------------------------------------------
    # The delivery semantics above are about WHEN you commit; these are the
    # concrete APIs for HOW, each trading throughput against safety.
    #
    # 1) AUTO COMMIT  (enable_auto_commit=True, we turned it OFF)
    #    The client commits in the BACKGROUND every auto_commit_interval_ms
    #    (default 5000 ms). Zero code, but the commit is tied to poll(), not to
    #    your processing finishing -> if you crash mid-processing after an
    #    auto-commit already fired, that record is skipped (at-most-once-ish).
    #
    # 2) MANUAL SYNC  -> consumer.commit()
    #    Blocks until the broker acknowledges the commit, and RETRIES on
    #    retriable errors. Safest, but pays a blocking round-trip per call.
    #
    # 3) MANUAL ASYNC -> consumer.commit_async()          <-- hot path below
    #    Returns immediately; does NOT retry (a later commit supersedes it).
    #    Higher throughput, a lost async commit near a crash just re-reads a few
    #    records (still at-least-once, our OpenSearch sink is idempotent anyway).
    #
    # 4) BATCH / PERIODIC commit -> commit once every N records / T seconds.
    # 5) EXPLICIT OFFSETS -> consumer.commit({TopicPartition: OffsetAndMetadata}).
    #
    # PRODUCTION PATTERN (applied here): commit_async() on the hot path for
    # speed, then a final synchronous commit() in the finally block so the last
    # offsets are durably stored before the consumer leaves the group.
    # Per-record TRANSIENT retry counter, keyed by (partition, offset). A single
    # consumer thread processes one record at a time and seeks back on failure,
    # so the same (partition, offset) comes around again and we increment its
    # count until it succeeds or hits MAX_RETRIES.
    retry_counts = {}

    try:
        # never exits under load: keeps waiting for and consuming new messages
        for record in consumer:
            # 1) build a handler for THIS record and let it process the message.
            handler = handler_cls(
                key=record.key,
                value=record.value,
                headers=record.headers,
                group_id=group_id,
                consumer_name=consumer_name,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )
            record_key = (record.partition, record.offset)

            # -----------------------------------------------------------------
            # HANDLING ERRORS IN YOUR OWN PROCESSING LOGIC (handler.consume())
            # -----------------------------------------------------------------
            # Golden rule: only COMMIT records we actually handled. A commit must
            # never advance past a record whose processing failed, or that record
            # is silently lost on restart.
            try:
                handler.consume()
            except PoisonMessage as exc:
                # PERMANENT error: this record can NEVER succeed (bad data). Do
                # NOT retry -> dead-letter it straight away and commit past it.
                print(
                    f"[{group_id}/{consumer_name}] POISON "
                    f"{record.topic}-{record.partition}@{record.offset}: {exc}"
                )
                if dead_letter_topic:
                    _send_to_dead_letter(dead_letter_topic, record, exc)
            except Exception as exc:
                # TRANSIENT / unexpected error (sink down, network blip): re-read
                # will likely succeed later. Retry up to MAX_RETRIES times, each
                # time seeking back to THIS offset and WITHOUT committing, so
                # at-least-once holds (nothing committed -> nothing lost).
                attempts = retry_counts.get(record_key, 0) + 1
                retry_counts[record_key] = attempts

                if attempts <= MAX_RETRIES:
                    print(
                        f"[{group_id}/{consumer_name}] TRANSIENT error on "
                        f"{record.topic}-{record.partition}@{record.offset}, "
                        f"retry {attempts}/{MAX_RETRIES}: {exc}"
                    )
                    consumer.seek(
                        TopicPartition(record.topic, record.partition),
                        record.offset,
                    )
                    time.sleep(RETRY_BACKOFF_SECONDS)
                    # skip the commit below; re-poll and process this record again.
                    continue

                # retries exhausted -> stop blocking the partition: dead-letter
                # the record and fall through to COMMIT past it.
                print(
                    f"[{group_id}/{consumer_name}] EXHAUSTED {MAX_RETRIES} retries "
                    f"on {record.topic}-{record.partition}@{record.offset}, "
                    f"dead-lettering: {exc}"
                )
                if dead_letter_topic:
                    _send_to_dead_letter(dead_letter_topic, record, exc)
                retry_counts.pop(record_key, None)
            else:
                # success -> drop any retry bookkeeping for this record.
                retry_counts.pop(record_key, None)

            # 2) handled (success / poisoned / dead-lettered) -> ASYNC commit on
            #    the hot path: fire-and-forget so the loop is not blocked by a
            #    network round-trip on every record. It commits the offsets
            #    already returned by poll(), up to and including this one.
            consumer.commit_async()
            
            
            
    except KeyboardInterrupt:
        # Ctrl-C -> fall through to the graceful shutdown in finally.
        print(f"[{group_id}/{consumer_name}] interrupted, shutting down...")
    finally:
        # Final SYNC commit: unlike commit_async(), this BLOCKS and RETRIES, so
        # the offsets from the last processed records are durably stored before
        # we exit (any in-flight async commit gives no such guarantee). Then
        # close() leaves the group cleanly so Kafka rebalances the partitions to
        # the remaining consumers immediately instead of after a timeout.
        try:
            consumer.commit()
        finally:
            consumer.close()
            print(f"[{group_id}/{consumer_name}] closed.")


def run_bulk_consumer(
    group_id: str,
    consumer_name: str,
    topic_name: str,
    build_action,
    bulk_write,
    auto_offset_reset="earliest",
    dead_letter_topic: Optional[str] = None,
):
    """Batch variant of run_consumer that writes to the sink in BULK.

    Instead of one record at a time, it poll()s up to BULK_MAX_RECORDS and writes
    them in a single bulk request -> far fewer round-trips = higher throughput.

    Callbacks provided by the caller (keeps this sink-agnostic):
      build_action(record) -> (doc_id, doc)
          Turn a Kafka record into an id + document. Raise PoisonMessage for data
          that can NEVER be written (e.g. bad JSON) -> it is dead-lettered and
          excluded from the batch.
      bulk_write(list[(doc_id, doc)]) -> list of per-document failures
          Write the batch. RAISE on a transport failure (whole batch transient);
          RETURN per-document errors (each a dict {op: {status, _id, error}}) for
          docs the server rejected individually.

    Offsets are committed only AFTER the whole polled batch is handled, so a crash
    replays the batch. With an idempotent sink (deterministic _id) the replay just
    overwrites -> at-least-once stays correct.
    """
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        # cap how many records one poll() hands us -> bounds the bulk size and
        # keeps each loop well within max.poll.interval.ms (no group eviction).
        max_poll_records=BULK_MAX_RECORDS,
        key_deserializer=DefaultSerializer(),
        value_deserializer=DefaultSerializer(),
    )

    print(
        f"[{group_id}/{consumer_name}] BULK consumer started "
        f"(<= {BULK_MAX_RECORDS} records/batch), polling forever..."
    )

    try:
        while True:
            # poll() returns {TopicPartition: [records]}; {} on an idle timeout,
            # which also gives KeyboardInterrupt a chance to fire on shutdown.
            batch = consumer.poll(timeout_ms=BULK_POLL_TIMEOUT_MS)
            if not batch:
                continue

            polled = sum(len(records) for records in batch.values())

            # 1) build docs; dead-letter poison records and exclude them.
            doc_by_id = {}       # doc_id -> doc (what we send to bulk_write)
            record_by_id = {}    # doc_id -> record (to dead-letter on failure)
            for _tp, records in batch.items():
                for record in records:
                    try:
                        doc_id, doc = build_action(record)
                    except PoisonMessage as exc:
                        print(
                            f"[{group_id}/{consumer_name}] POISON "
                            f"{record.topic}-{record.partition}@{record.offset}: {exc}"
                        )
                        if dead_letter_topic:
                            _send_to_dead_letter(dead_letter_topic, record, exc)
                        continue
                    doc_by_id[doc_id] = doc
                    record_by_id[doc_id] = record

            # 2) bulk-write with retry. `pending` shrinks to only the docs that
            #    still need writing (transient failures) each round.
            pending_ids = list(doc_by_id.keys())
            attempts = 0
            while pending_ids:
                pairs = [(doc_id, doc_by_id[doc_id]) for doc_id in pending_ids]
                try:
                    errors = bulk_write(pairs)
                except Exception as exc:
                    # transport failure -> the WHOLE batch is transient.
                    attempts += 1
                    if attempts <= MAX_RETRIES:
                        print(
                            f"[{group_id}/{consumer_name}] bulk TRANSPORT error, "
                            f"retry {attempts}/{MAX_RETRIES}: {exc}"
                        )
                        time.sleep(RETRY_BACKOFF_SECONDS)
                        continue
                    # exhausted -> dead-letter everything still pending, give up.
                    print(
                        f"[{group_id}/{consumer_name}] bulk EXHAUSTED "
                        f"{MAX_RETRIES} retries, dead-lettering {len(pending_ids)} docs"
                    )
                    if dead_letter_topic:
                        for doc_id in pending_ids:
                            _send_to_dead_letter(dead_letter_topic, record_by_id[doc_id], exc)
                    break

                # split per-document errors: PERMANENT -> DLQ, TRANSIENT -> retry.
                transient_ids = []
                for err in errors:
                    op = next(iter(err.values()))          # {"index": {...}}
                    status = op.get("status", 500)
                    doc_id = op.get("_id")
                    reason = op.get("error")
                    if _bulk_error_is_transient(status):
                        transient_ids.append(doc_id)
                    else:
                        print(
                            f"[{group_id}/{consumer_name}] bulk PERMANENT error "
                            f"(status {status}) on _id={doc_id}: {reason}"
                        )
                        if dead_letter_topic:
                            _send_to_dead_letter(
                                dead_letter_topic, record_by_id[doc_id], Exception(str(reason))
                            )

                if not transient_ids:
                    break  # batch fully resolved (successes + dead-lettered)

                attempts += 1
                if attempts > MAX_RETRIES:
                    print(
                        f"[{group_id}/{consumer_name}] bulk EXHAUSTED retries on "
                        f"{len(transient_ids)} docs, dead-lettering"
                    )
                    if dead_letter_topic:
                        for doc_id in transient_ids:
                            _send_to_dead_letter(
                                dead_letter_topic, record_by_id[doc_id],
                                Exception("bulk transient retries exhausted"),
                            )
                    break

                print(
                    f"[{group_id}/{consumer_name}] retrying {len(transient_ids)} "
                    f"transient docs, attempt {attempts}/{MAX_RETRIES}"
                )
                pending_ids = transient_ids
                time.sleep(RETRY_BACKOFF_SECONDS)

            # 3) whole polled batch handled -> commit its offsets (async).
            #    Print a one-line progress summary so it is VISIBLE the consumer
            #    is working (bulk success is otherwise silent). poison = bad data
            #    excluded before the bulk; indexed = docs sent to the bulk write.
            poison = polled - len(doc_by_id)
            print(
                f"[{group_id}/{consumer_name}] batch: polled={polled} "
                f"indexed={len(doc_by_id)} poison={poison}"
            )
            consumer.commit_async()
    except KeyboardInterrupt:
        print(f"[{group_id}/{consumer_name}] interrupted, shutting down...")
    finally:
        try:
            consumer.commit()
        finally:
            consumer.close()
            print(f"[{group_id}/{consumer_name}] closed.")
