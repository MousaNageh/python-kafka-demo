from abc import ABC, abstractmethod


class PoisonMessage(Exception):
    """Raised by a handler when a record can NEVER be processed successfully
    (bad/corrupt data, missing required fields, ...).

    It tells run_consumer: do NOT retry this record -> retrying a poison message
    would block the partition forever. Instead the loop logs/dead-letters it and
    commits PAST it so the consumer keeps making progress.

    Any OTHER exception a handler raises is treated as TRANSIENT (e.g. the sink
    is temporarily down): run_consumer does NOT commit, seeks back, and re-reads
    the record until it succeeds.
    """


class ConsumerHandler(ABC):
    """Abstract per-record handler.

    `run_consumer` builds ONE instance of a subclass for every message it
    reads, handing it the record's data (key, value, headers, ...) plus the
    owning consumer's identity (group_id, consumer_name). Subclasses only have
    to implement `consume()` with whatever processing they need.

    Flow enforced by the consumer loop:
        read record -> handler = Handler(...) -> handler.consume() -> commit
    i.e. the offset is committed AFTER `consume()` returns (at-least-once).
    """

    def __init__(
        self,
        key,
        value,
        headers,
        group_id: str,
        consumer_name: str,
        topic: str,
        partition: int,
        offset: int,
    ):
        self.key = key
        self.value = value
        self.headers = headers
        self.group_id = group_id
        self.consumer_name = consumer_name
        self.topic = topic
        self.partition = partition
        self.offset = offset

    @abstractmethod
    def consume(self):
        """Process the record. Runs BEFORE the offset is committed."""
        raise NotImplementedError
