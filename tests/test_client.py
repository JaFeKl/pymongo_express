import os
from pymongo_express import Client

db = Client(
    os.environ.get("MONGODB_URL"),
    os.environ.get("MONGODB_USER"),
    os.environ.get("MONGODB_PASSWORD"),
    os.environ.get("MONGODB_PORT"),
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
    id, collection = db.create_entry(example_entry, collectionName, databaseName)

    assert id is not None
    assert collection is not None

    mydatabase = db.get_database_by_name(databaseName)
    assert mydatabase is not None
    assert mydatabase.name == databaseName

    result = db.collection_exists(collectionName, databaseName)
    assert result is True

    mycollection = db.get_collection_by_name(collectionName, databaseName)
    assert mycollection is not None
    assert mycollection.name == collectionName

    entry = db.get_entry_by_id(id, collectionName)
    assert entry == example_entry

    result = db.match_entry(
        databaseName, collectionName, {"name": "test", "description": "myDescription"}
    )
    assert result is not None
    assert len(result) == 1
    assert result[0]["name"] == "test"

    result = db.delete_entry_by_id(id, collectionName)

    assert result is True

    result = db.delete_collection(collectionName)

    assert result is True
