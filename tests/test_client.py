import os
import logging
from pymongo_express import PymongoExpressClient
from pymongo.collection import Cursor

LOGGER = logging.getLogger(__name__)


handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))

LOGGER.addHandler(handler)
LOGGER.setLevel(logging.WARNING)


client = PymongoExpressClient(
    os.environ.get("MONGODB_URL"),
    os.environ.get("MONGODB_USER"),
    os.environ.get("MONGODB_PASSWORD"),
    os.environ.get("MONGODB_PORT"),
    logger=LOGGER,
)

databaseName = "myTestDatabase"
collectionName = "myTestCollection"

example_entry = {
    "name": "test",
    "description": "myDescription",
    "type": "myType",
    "created_at": "2023-10-01T00:00:00Z",
}


def test_client():
    id, collection = client.create_entry(example_entry, collectionName, databaseName)

    assert id is not None
    assert collection is not None

    mydatabase = client.get_database_by_name(databaseName)
    assert mydatabase is not None
    assert mydatabase.name == databaseName

    result = client.collection_exists(collectionName, databaseName)
    assert result is True

    mycollection = client.get_collection_by_name(collectionName, databaseName)
    assert mycollection is not None
    assert mycollection.name == collectionName

    entry = client.get_entry_by_id(id, collectionName)
    assert entry == example_entry

    result = client.match_entry(
        databaseName, collectionName, {"name": "test", "description": "myDescription"}
    )
    assert result is not None
    assert len(result) == 1
    assert result[0]["name"] == "test"

    # queries
    query = client.query_get_entries_by_ids([id])
    results: Cursor = mycollection.find(query)
    assert len(results.to_list()) == 1

    result = client.delete_entry_by_id(id, collectionName)

    assert result is True

    result = client.delete_collection(collectionName)

    assert result is True
