import threading

from opensearchpy import OpenSearch, helpers

# The security plugin is DISABLED on the container (see docker-compose.kraft.yml),
# so the REST API is plain HTTP on localhost:8060 with no auth.
OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 8060

# Name of the index the wikimedia changes are written into.
WIKIMEDIA_INDEX = "wikimedia"


# One shared client for the whole process. The OpenSearch client keeps an
# internal urllib3 connection pool and is thread-safe, so all consumer threads
# reuse this single instance instead of each opening its own connections.
_client = None
_client_lock = threading.Lock()


def get_client():
    """Return the process-wide OpenSearch client, creating it on first use."""
    global _client
    if _client is None:
        # double-checked locking: multiple consumer threads may call this at the
        # same moment on startup -> only the first one builds the client.
        with _client_lock:
            if _client is None:
                _client = OpenSearch(
                    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
                    # plain HTTP, no TLS, no auth (security plugin is off)
                    use_ssl=False,
                    verify_certs=False,
                )
    return _client


def ensure_index(index_name: str = WIKIMEDIA_INDEX):
    """Create the index if it does not already exist (idempotent, safe to call
    on every startup). Returns True if it was created, False if it existed."""
    client = get_client()
    if client.indices.exists(index=index_name):
        print(f"OpenSearch index '{index_name}' already exists.")
        return False
    client.indices.create(index=index_name)
    print(f"Created OpenSearch index '{index_name}'.")
    return True


def index_document(document: dict, doc_id: str, index_name: str = WIKIMEDIA_INDEX):
    """Write one document, using doc_id as the OpenSearch _id.

    Passing an explicit _id makes the write IDEMPOTENT: because our Kafka
    consumer is at-least-once, the same record can be re-processed after a crash
    -> re-indexing with the same _id OVERWRITES the existing document instead of
    creating a duplicate, which keeps the sink correct.
    """
    return get_client().index(index=index_name, body=document, id=doc_id)


def bulk_index(id_doc_pairs, index_name: str = WIKIMEDIA_INDEX):
    """Index many (doc_id, doc) pairs in ONE request -> far fewer round-trips
    than index_document per record, the main throughput win for the consumer.

    Each pair uses _op_type "index" (the default) so writing the same _id again
    OVERWRITES -> still idempotent under at-least-once replay.

    Returns the list of PER-DOCUMENT failures (each a dict like
    {"index": {"status": 400, "_id": ..., "error": {...}}}). A TRANSPORT failure
    (OpenSearch unreachable) is RAISED instead, so the caller can treat the whole
    batch as transient and retry it:
      - raise_on_exception=True -> connection/transport errors bubble up.
      - raise_on_error=False    -> per-document errors do NOT raise; they come
                                   back in the returned list so the caller can
                                   route just those docs to the DLQ.
    """
    actions = [
        {"_index": index_name, "_id": doc_id, "_source": doc}
        for doc_id, doc in id_doc_pairs
    ]
    _success, errors = helpers.bulk(
        get_client(),
        actions,
        raise_on_exception=True,
        raise_on_error=False,
    )
    return errors
