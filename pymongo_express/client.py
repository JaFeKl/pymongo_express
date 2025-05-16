import logging
from typing import List, Optional, Tuple, Union, List
import bson

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

LOGGER = logging.getLogger(__name__)


class Client:
    def __init__(
        self, url: str, username: str, password: str, port: int = 27017, logger=None
    ) -> None:
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self._client = MongoClient(f"mongodb://{username}:{password}@{url}:{port}/")
        try:
            self._client.admin.command("ismaster")
        except OperationFailure as e:
            print("PyMongo client is not connected to MongoDB:", e)
            return
        self.logger.info("Initialized Database client")

    def database_exists(self, databaseName: str) -> bool:
        """
        Check if a database with the specified name exists on the MongoDB server.

        Args:
            databaseName (str): The name of the database to check for existence.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        return databaseName in self._client.list_database_names()

    def get_database_by_name(self, databaseName: str) -> Optional[Database]:
        """
        Retrieve a database by its name if it exists.

        Args:
            databaseName (str): The name of the database to retrieve.

        Returns:
            Optional[Database]: The Database object if it exists, otherwise None.

        Logs:
            Emits a warning if the specified database does not exist.
        """
        if self.database_exists(databaseName):
            return self._client[databaseName]
        else:
            self.logger.warning(f"Database '{databaseName}' does not exist.")
            return None

    def collection_exists(self, collectionName: str, databaseName: str = None) -> bool:
        """
        Check if a collection exists in the specified database or in any database.

        Args:
            collectionName (str): The name of the collection to check for existence.
            databaseName (str, optional): The name of the database to search in.
                If None, searches all databases. Defaults to None.

        Returns:
            bool: True if the collection exists, False otherwise.

        Logs:
            - Debug message if the collection is found.
            - Warning message if the collection or database does not exist.
        """
        if databaseName is None:
            databases = self._client.list_database_names()
            for db in databases:
                collections = self._client[db].list_collection_names()
                if collectionName in collections:
                    self.logger.debug(
                        f"Collection '{collectionName}' exists in database '{db}'"
                    )
                    return True
            return False
        else:
            db = self.get_database_by_name(databaseName)
            if db is not None:
                collections = db.list_collection_names()
                if collectionName in collections:
                    self.logger.debug(
                        f"Collection '{collectionName}' exists in database '{databaseName}'"
                    )
                    return True
                else:
                    self.logger.warning(
                        f"Collection '{collectionName}' does not exist in database '{databaseName}'"
                    )
                    return False
            else:
                self.logger.warning(
                    f"Database '{databaseName}' does not exist, cannot check for collection '{collectionName}'"
                )
                return False

    def get_collection_by_name(
        self, collectionName: str, databaseName: str = None
    ) -> Optional[Collection]:
        dbs: List[Database] = []
        if databaseName is not None:
            db = self.get_database_by_name(databaseName)
            if db is not None:
                dbs.append(db)
        else:  # we search for the collection in all databases
            db_names = self._client.list_database_names()
            # Exclude 'admin' and 'local' databases
            db_names = [name for name in db_names if name not in ["config", "local"]]
            for db_name in db_names:
                dbs.append(self._client[db_name])
        for db in dbs:
            try:
                collections = db.list_collection_names()
                if collectionName in collections:
                    LOGGER.debug(
                        f"Retrieved collection '{collectionName}' from database '{db.name}'"
                    )
                    return db[collectionName]
            except OperationFailure as e:
                LOGGER.debug(
                    f"Unable to retrieve collection from database '{db.name}': {e}"
                )
        LOGGER.warning(
            f"Unable to retrieve collection named '{collectionName}' in databases {[db.name for db in dbs]}"
        )
        return None

    def get_entry_by_id(
        self,
        _id: Union[str, bson.ObjectId],
        collectionName: str,
        databaseName: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Retrieve a document by its _id from the specified collection and database.

        Args:
            _id (Union[str, bson.ObjectId]): The _id of the document to retrieve.
            collectionName (str): The name of the collection.
            databaseName (str, optional): The name of the database. If not provided, searches all databases.

        Returns:
            Optional[dict]: The document if found, otherwise None.
        """
        if collectionName is None:
            self.logger.warning("No collectionName provided to get_entry_by_id.")
            return None

        collection = self.get_collection_by_name(collectionName, databaseName)
        if collection is None:
            self.logger.warning(f"Collection '{collectionName}' not found.")
            return None

        # Convert _id to ObjectId if it's a string
        if isinstance(_id, str):
            try:
                _id = bson.ObjectId(_id)
            except Exception as e:
                self.logger.warning(f"Invalid _id format: {_id}. Error: {e}")
                return None

        entry = collection.find_one({"_id": _id})
        if entry is None:
            self.logger.info(
                f"No entry found with _id '{_id}' in collection '{collectionName}'."
            )
        return entry

    def match_entry(
        self,
        database_name: str,
        collection_name: str,
        key_values: dict,
    ) -> Optional[List[dict]]:
        collection = self.get_collection_by_name(collection_name, database_name)
        if collection is not None:
            query = {}
            for key, value in key_values.items():
                query = self._get_entries_with_value(key, value, query=query)
            results = collection.find(query)
            return [result for result in results]
        else:
            return None

    def create_entry(
        self,
        data: dict,
        collection_name: str,
        database_name: str,
    ) -> Optional[Tuple[str, Collection]]:
        db: Database = self._client[database_name]
        collection: Collection = db[collection_name]
        result = collection.insert_one(data)
        return result.inserted_id, collection

    def delete_entry_by_id(
        self,
        id: Union[bson.ObjectId, str],
        collection_name: str,
        database_name: str = None,
    ) -> bool:
        collection = self.get_collection_by_name(collection_name, database_name)
        if collection is None:
            LOGGER.warning(f"Collection '{collection_name}' not found.")
            return False
        result = collection.delete_one({"_id": id})
        if result.deleted_count > 0:
            LOGGER.info(
                f"Deleted entry with _id '{id}' from collection '{collection_name}'"
            )
            return True
        else:
            LOGGER.warning(
                f"No entry found with _id '{id}' in collection '{collection_name}'"
            )
            return False

    def delete_collection(
        self,
        collection_name: str,
        database_name: str = None,
    ) -> bool:
        """
        Deletes a collection from the specified database.

        Args:
            collection_name (str): The name of the collection to delete.
            database_name (str, optional): The name of the database. If not provided, searches all databases.

        Returns:
            bool: True if the collection was deleted successfully, False otherwise.
        """
        collection = self.get_collection_by_name(collection_name, database_name)
        if collection is None:
            LOGGER.warning(f"Collection '{collection_name}' not found.")
            return False
        collection.drop()

        collection = self.get_collection_by_name(collection_name, database_name)
        if collection:
            LOGGER.warning(f"Failed to delete collection '{collection_name}'")
            return False
        else:
            LOGGER.info(f"Deleted collection '{collection_name}'")
            return True

    def update_entry(
        self,
        _id: Union[bson.ObjectId, str],
        data: dict,
        collection_name: str,
        database_name: str,
    ):
        db: Database = self._client[database_name]
        collection: Collection = db[collection_name]
        result = collection.update_one(
            {"_id": _id},
            {"$set": data},
        )
        if result.modified_count > 0:
            LOGGER.info(
                f"Updated entry with _id '{_id}' in collection '{collection_name}'"
            )
        else:
            LOGGER.warning(
                f"No update on entry  entry found with _id '{_id}' in collection '{collection_name}'"
            )

    def create_or_update_entry(
        self,
        data: dict,
        collection_name: str,
        database_name: str,
    ) -> Optional[Tuple[str, Collection]]:
        """
        Creates a new entry or updates an existing entry in the specified MongoDB collection.

        If the provided data dictionary does not contain an "_id" field, a new entry is created.
        If the "_id" field is present, the corresponding document is updated with the new data.
        If no document with the given "_id" exists, a new document is inserted (upsert).

        Args:
            data (dict): The data to insert or update in the collection.
            collection_name (str): The name of the MongoDB collection.
            database_name (str): The name of the MongoDB database.

        Returns:
            Optional[Tuple[str, Collection]]: A tuple containing the ID of the created or updated entry
            and the Collection object. Returns None if the operation fails.
        """

        db: Database = self._client[database_name]
        collection: Collection = db[collection_name]

        if "_id" not in data:
            id, _ = self.create_entry(data, collection_name, database_name)
            return id, collection
        else:
            result = collection.update_one(
                {"_id": data["_id"]},
                {"$set": data},
                upsert=True,
            )
            return result.upserted_id, collection

    def get_all_ids_in_collection(self, collection: Collection) -> List[str]:
        return collection.distinct("_id")

    def get_most_recent_entry(
        self, collectionName: str, databaseName: Optional[str] = None
    ) -> Optional[dict]:
        """
        Retrieve the most recent entry from a specified MongoDB collection.
        Args:
            collectionName (str): The name of the collection to query.
            databaseName (Optional[str], optional): The name of the database containing the collection.
                If not provided, the default database is used.
        Returns:
            Optional[dict]: The most recent document from the collection, or None if the collection
                does not exist or contains no entries.
        Logs:
            - A warning if the collection is not found or contains no entries.
            - An info message with the most recent entry if found.
        """
        collection = self.get_collection_by_name(collectionName, databaseName)
        if collection is None:
            LOGGER.warning(f"Collection '{collectionName}' not found.")
            return None
        else:
            result = collection.find_one(sort=[("_id", -1)])
            if result is None:
                LOGGER.warning(f"No entries found in collection '{collectionName}'.")
            else:
                LOGGER.info(
                    f"Most recent entry in collection '{collectionName}': {result}"
                )
            return result

    def query_get_entries_by_ids(
        self, ids: List[Union[str, bson.ObjectId]], query: dict = None
    ) -> dict:
        """
        Updates the provided MongoDB query dictionary to filter documents by a list of IDs.

        Args:
            ids (List[Union[str, bson.ObjectId]]): A list of document IDs, each as a string or bson.ObjectId.
            query (dict, optional): An existing MongoDB query dictionary to update. If None, an error may occur.

        Returns:
            dict: The updated query dictionary with an "_id" filter for the provided IDs.

        Raises:
            TypeError: If any element in `ids` is not a str or bson.ObjectId.

        Note:
            The function modifies the `query` dictionary in place by adding or updating the "_id" key.
        """
        object_ids = []
        for id in ids:
            if isinstance(id, str):
                object_ids.append(bson.ObjectId(id))
            elif isinstance(id, bson.ObjectId):
                object_ids.append(id)
            else:
                raise TypeError(
                    f"Invalid type for id: {type(id)}. Expected str or bson.ObjectId."
                )
        query["_id"] = {"$in": object_ids}
        return query

    def query_get_entries_where_key_exists(self, key: str, query: dict = None) -> dict:
        """
        Constructs and returns a MongoDB query dictionary that matches documents where the specified key exists.
        Args:
            key (str): The field name to check for existence in the documents.
            query (dict, optional): An existing query dictionary to extend. Defaults to None.
        Returns:
            dict: The updated query dictionary with the condition that the specified key must exist.
        """
        if query is None:
            query = {}
        query.setdefault(key, {})
        query[key]["$exists"] = True
        return query

    def query_get_entries_where_key_not_exists(
        self, key: str, query: dict = None
    ) -> dict:
        """
        Constructs a MongoDB query to find documents where a specified key does not exist.
        Args:
            key (str): The field name to check for non-existence in the documents.
            query (dict, optional): An existing query dictionary to update. Defaults to None.
        Returns:
            dict: The updated query dictionary with the condition that the specified key does not exist.
        """
        if query is None:
            query = {}
        query.setdefault(key, {})
        query[key]["$exists"] = False
        return query

    def query_get_entries_with_value(self, key: str, value, query: dict = None) -> dict:
        """
        Constructs and returns a MongoDB query dictionary that matches documents where the specified key has the given value.
        If an initial query dictionary is provided, it is updated with the key-value pair; otherwise, a new query dictionary is created.
        Args:
            key (str): The field name to match in the query.
            value: The value that the specified field should have.
            query (dict, optional): An existing query dictionary to update. Defaults to None.
        Returns:
            dict: The resulting query dictionary with the added key-value condition.
        """

        if query is None:
            query = {}
        query[key] = value
        return query

    def query_get_entries_with_value_range(
        self, key: str, value: List[float], query: dict = None
    ) -> dict:
        """
        Constructs and returns a MongoDB query dictionary that filters documents where the specified key's value falls within a given range.
        Args:
            key (str): The field name to filter on.
            value (List[float]): A list containing two float values [min, max] representing the inclusive lower and upper bounds of the range.
            query (dict, optional): An existing query dictionary to update. If None, a new dictionary is created.
        Returns:
            dict: The updated query dictionary with the range filter applied to the specified key.
        """
        if query is None:
            query = {}
        query[key] = {
            "$gte": value[0],
            "$lte": value[1],
        }  # greater than or equal, lesser than or equal
        return query
