
from kafka_managers.consumer_handler import PoisonMessage
import multiprocessing as mp
from kafka_managers.consumers import run_bulk_consumer
import json

from opensearch_managers.opensearch_client import ensure_index, bulk_index
from kafka_managers.topic_manager import create_topic
from kafka_managers.producer import dlq_topic_for

from topics import WIKIMEDIA_RECENT_TOPIC


def extract_id(data: dict) -> str:
    """Pick a STABLE, unique id for the document so re-processing the same Kafka
    record (at-least-once may deliver twice) overwrites instead of duplicating.

    Wikimedia recentchange events carry a unique event id at data["meta"]["id"];
    we fall back to the recentchange "id" if meta is missing.
    """
    meta = data.get("meta") or {}
    return meta.get("id") or str(data.get("id"))


def build_action(record):
    """Kafka record -> (doc_id, doc) for the bulk writer.

    Raise PoisonMessage for a payload that can NEVER be indexed (bad JSON) so the
    bulk consumer dead-letters it and leaves it out of the batch.
    """
    try:
        data = json.loads(record.value)
    except json.JSONDecodeError as exc:
        raise PoisonMessage(f"invalid JSON: {exc}") from exc
    return extract_id(data), data


def bulk_write(pairs):
    """Write a batch of (doc_id, doc) to OpenSearch in one bulk request.

    Returns the list of per-document failures; a transport failure (OpenSearch
    down) RAISES so the bulk consumer retries the whole batch as transient.
    """
    return bulk_index(pairs)


GROUP_ID = "wikimedia.recentchange.group_1"


def worker(consumer_name: str, auto_offset_reset: str, dead_letter_topic: str):
    """Entry point run inside EACH child process -> one consumer per process.

    Because this runs in the child's OWN main thread, a Ctrl-C / SIGINT sent to
    the process group reaches it directly and raises KeyboardInterrupt here, so
    the consumer's finally (final commit + close) actually runs on shutdown.
    Kafka/OpenSearch clients are created lazily INSIDE the child, so no network
    connections are shared across processes.
    """
    run_bulk_consumer(
        GROUP_ID,
        consumer_name,
        WIKIMEDIA_RECENT_TOPIC,
        build_action,
        bulk_write,
        auto_offset_reset,
        dead_letter_topic,
    )


if __name__ == "__main__":
    # "spawn" (not the Linux default "fork"): each child starts as a FRESH Python
    # process that re-imports this module and builds its OWN clients. This avoids
    # inheriting the parent's OpenSearch connection (created by ensure_index
    # below) across a fork, which would corrupt/hang those sockets.
    mp.set_start_method("spawn")

    # dead-letter topic name derived from the source topic (single source of
    # truth shared with the producer): "wikimedia.recentchange.dlq".
    dlq_topic = dlq_topic_for(WIKIMEDIA_RECENT_TOPIC)

    # make sure the target index exists before any consumer starts writing.
    ensure_index()
    # make sure the dead-letter topic exists before any consumer routes to it.
    create_topic(WIKIMEDIA_RECENT_TOPIC, num_partitions=3, replication_factor=1)
    create_topic(dlq_topic, num_partitions=3, replication_factor=1)

    # one consumer PER PROCESS, all sharing GROUP_ID -> Kafka splits the topic's
    # partitions across them (the same way you would scale with N containers).
    procs = [
        mp.Process(target=worker, args=(f"{GROUP_ID}.consumer_{i}", "earliest", dlq_topic))
        for i in range(1, 4)
    ]

    for p in procs:
        p.start()

    try:
        # wait for the children. On Ctrl-C the SIGINT also hits every child (same
        # process group), so each one shuts down cleanly on its own.
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        # the parent got SIGINT too -> don't exit yet. The children are already
        # running their graceful shutdown; join again to let them finish.
        print("Shutting down consumers...")
        for p in procs:
            p.close()
