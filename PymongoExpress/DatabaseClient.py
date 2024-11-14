import os
import pprint
import logging
import json
import pandas as pd
from bson import BSON, encode
from typing import List, Optional, Tuple, Union, Dict
from functools import reduce
from datetime import datetime
import bson
# from bson import ObjectId, json_util
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.cursor import Cursor
from pymongo.errors import PyMongoError, OperationFailure
from pymongo.collection import Collection


# https://stackoverflow.com/questions/25833613/safe-method-to-get-value-of-nested-dictionary
def deep_get(dictionary, keys, default=None):
    return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), dictionary)


class DatabaseClient():
    def __init__(self,
                 url: str,
                 username: str,
                 password: str,
                 port: int = 27017) -> None:
        self.logger = logging.getLogger(__name__)
        self.myclient = MongoClient(f"mongodb://{username}:{password}@{url}:{port}/")
        try:
            self.myclient.admin.command('ismaster')
        except OperationFailure as e:
            print("PyMongo client is not connected to MongoDB:", e)
            return
        #print(self._get_existing_collection_by_name("Benchmark", "MaterialTransfer"))
        self.get_entry_by_id("65c3ab3a9a794b03a576cb70")

        # self.db_name = "MaterialTransfer"
        # self.mydb = self.myclient[self.db_name]
        # self.experiments = self.mydb["Experiments"]
        # self.experimentsTest = self.mydb["Experiments_Tests"]
        # self.experimentsSim = self.mydb["ExperimentsSim"]
        # self.experimentsSimTest = self.mydb["ExperimentsSim_Tests"]
        # self.benchmark = self.mydb["Benchmark"]
        # self.benchmarkTest = self.mydb["Benchmark_Tests"]
        # self.benchmarkRuns = self.mydb["BenchmarkRuns"]
        # self.benchmarkRunsTest = self.mydb["BenchmarkRuns_Test"]
        # self.realDoe = self.mydb["RealDoe"]
        # self.realDoeTest = self.mydb["RealDoeTest"]
        # self.sim360 = self.mydb["Sim360"]
        # self.sim360Test = self.mydb["Sim360_Test"]
        # self.determinism = self.mydb["Determinism"]
        # self.determinismTest = self.mydb["Determinism_Test"]
        self.logger.info("Initialized Database client")

    def _get_existing_db_by_name(self, databaseName: str):
        if databaseName not in self.myclient.list_database_names():
            self.logger.warning(f"Database {databaseName} does not exist, please create database first")
            return None
        else:
            return self.myclient[databaseName]

    def _get_existing_collection_by_name(self, collectionName: str, databaseName: str = None) -> Union[dict, None]:
        """
        Retrieves an existing collection by name from the specified database, or from tries all databases if no database is specified.

        Parameters:
            collectionName (str): The name of the collection to retrieve.
            databaseName (str, optional): The name of the database to search for the collection.
                If not provided, the function searches for the collection in all databases except 'config' and 'local'.

        Returns:
            Union[dict, None]: A dictionary containing the database name and the collection object if found, 
                or None if the collection is not found in any of the databases.

        Raises:
            None
        """
        result = {}
        dbs: List[Database] = []
        if databaseName is not None:
            db = self._get_existing_db_by_name(databaseName)
            if db is not None:
                dbs.append(db)
        else:   # we search for the collection in all databases
            db_names = self.myclient.list_database_names()
            # Exclude 'admin' and 'local' databases
            db_names = [name for name in db_names if name not in ['config', 'local']]
            for db_name in db_names:
                dbs.append(self.myclient[db_name])
        for db in dbs:
            try:
                collections = db.list_collection_names()
                if collectionName in collections:
                    self.logger.debug(f"Retrieved collection '{collectionName}' from database '{db.name}'")
                    result["db_name"] = db.name
                    result["collection"] = db[collectionName]
                    return result
            except OperationFailure as e:
                self.logger.debug(f"Unable to retrieve collection from database '{db.name}': {e}")
        self.logger.warning(f"Unable to retrieve collection named '{collectionName}' in databases {[db.name for db in dbs]}")
        return None

    def _get_all_collections_by_db(self) -> dict:
        """
        Retrieves all collections from all databases except 'config' and 'local'.

        Returns:
            dict: A dictionary containing database names as keys and lists of collection objects as values.

        Raises:
            None
        """
        results = {}
        db_names = self.myclient.list_database_names()
        # Exclude 'admin' and 'local' databases
        db_names = [name for name in db_names if name not in ['config', 'local']]
        dbs = [self.myclient[db_name] for db_name in db_names]
        for db in dbs:
            try:
                collections_names = db.list_collection_names()
                if len(collections_names) > 0:
                    results[db.name] = [db[collection] for collection in collections_names]
            except OperationFailure as e:
                self.logger.debug(f"Unable to retrieve collection from database '{db.name}': {e}")
        return results

    def get_entry_by_id(self, entryId: str, collectionName: str = None, databaseName: str = None) -> Union[Dict, None]:
        """
        Retrieves an entry by its _id from the specified collection or from all collections if no collection is specified.

        Parameters:
            entryId (str): The _id of the entry to retrieve.
            collectionName (str, optional): The name of the collection to search for the entry.
                If not provided, the function searches for the entry in all collections.
            databaseName (str, optional): The name of the database to search for the collection.
                Required if `collectionName` is provided.

        Returns:
            Union[Dict, None]: A dictionary containing the retrieved entry, along with the collection name and database name,
                or None if the entry is not found.

        Raises:
            None
        """
        results = {}
        collections: List[Collection] = []
        if collectionName is not None:
            result = self._get_existing_collection_by_name(collectionName, databaseName)
            if result is not None:
                collections.append(result["collection"])
        else:
            for db_name, collections in self._get_all_collections_by_db().items():
                for collection in collections:
                    try:
                        entry = collection.find_one({'_id': bson.ObjectId(entryId)})
                        if entry is not None:
                            self.logger.debug(f"Retrieved _id '{entryId}' from collection '{collection.name}' in database '{db_name}'")
                            result["_id"] = entryId
                            result["collection_name"] = collection.name
                            result["db_name"] = db_name
                            result["entry"] = entry
                            return result
                    except bson.errors.InvalidId as e:
                        self.logger.error(f"Invalid Id: {e}")
                        break
        return None
        
            

    # def get_entry_by_id(self, collection: Collection, id: str) -> dict:
    #     return collection.find_one({'_id': ObjectId(id)})
        
        
                    # if db is not None:
            #     db.list_collection_names()


    # def add_docking_entry(self, date: datetime, stationName: str, x_error: float, y_error: float, z_error):
    #     my_dict = {"date": date, "stationName": stationName, "x_error": x_error, "y_error": y_error, "z_error": z_error}
    #     self.dockingErrorCollection.insert_one(my_dict)

    def get_entry_from_collection(self, collectionName: str, entryId: str, test: bool = False, sim: bool = False):
        collection = self.get_collection(collectionName, test, sim)
        entry = self.get_entry_by_id(collection, entryId)
        return entry, collection

    def get_collection(self, name: str, test: bool = False, sim: bool = False) -> Optional[Collection]:
        """
        Get test or real collection that matches a given name
        Possible names: "Benchmark", "BenchmarkRuns", "Experiment", "360", "real"
        """
        names = ["Benchmark", "BenchmarkRuns", "Experiment", "360", "real", "determ"]
        if name not in names:
            print(f"No database collection for key \"{name}\" exists, available keys are {names}")
            return None
        if test is False:
            if name == "Benchmark":
                return self.benchmark
            elif name == "BenchmarkRuns":
                return self.benchmarkRuns
            elif name == "360":
                return self.sim360
            elif name == "real":
                return self.realDoe
            elif name == "determ":
                return self.determinism
            elif name == "Experiment":
                if sim is True:
                    return self.experimentsSim
                else:
                    return self.experiments
        else:
            if name == "Benchmark":
                return self.benchmarkTest
            elif name == "BenchmarkRuns":
                return self.benchmarkRunsTest
            elif name == "360":
                return self.sim360Test
            elif name == "real":
                return self.realDoeTest
            elif name == "determ":
                return self.determinismTest
            elif name == "Experiment":
                if sim is True:
                    return self.experimentsSimTest
                else:
                    return self.experimentsTest

    def load_doe(self, pathToFile: str) -> pd.DataFrame:
        return pd.read_csv(pathToFile, index_col=0)

    def get_experiment_ids(self, benchmarkRunId: str, test: bool = False, sim: bool = False, targetStation: str = None) -> Optional[List[str]]:
        collection = self.get_collection("BenchmarkRuns", test=test, sim=sim)
        benchmarkRunEntry = self.get_entry_by_id(collection, benchmarkRunId)
        ids = []
        for idx in benchmarkRunEntry["experiments"]:
            experiment = benchmarkRunEntry["experiments"][idx]
            if targetStation is not None:
                if experiment["targetStation"] != targetStation:
                    continue
            if sim is False:
                ids.append(experiment["databaseEntry"])
            else:
                ids.append(experiment["simDatabaseEntry"])
        return ids

    def get_all_ids(self, collection: Collection) -> List[str]:
        return collection.distinct('_id')

    def add_benchmark_entry(self, data: dict, test: bool = False) -> Tuple[str, Collection]:
        """
        Add a benchmark entry to the database
        """
        return self._create_database_entry_in_collection("Benchmark", data, test, sim=True)

    def add_benchmark_run_entry(self, data: dict, test: bool = False) -> Tuple[str, Collection]:
        return self._create_database_entry_in_collection("BenchmarkRuns", data, test, sim=True)

    def add_experiment_entry(self, data: dict, sim: bool = False, test: bool = False):
        """
        Add an experiment outcome to the database
        """
        collection = self.get_collection("Experiment", test, sim)
        result = collection.insert_one(data)
        self.logger.debug("Added experiment entry to database")
        return result.inserted_id

    def add_sim360_entry(self, data: dict, test: bool = False):
        """
        Add an experiment outcome to the database
        """
        return self._create_database_entry_in_collection("360", data, test, True)

    def add_real_doe_entry(self, data: dict, test: bool = False):
        """
        Add a real world doe to the database
        """
        return self._create_database_entry_in_collection("real", data, test, False)

    def add_determinism_entry(self, data: dict, test: bool = False):
        """
        Add a determinism experiment to the database
        """
        return self._create_database_entry_in_collection("determ", data, test, sim=True)

    def _create_database_entry_in_collection(self, type_name: str, data: dict, test: bool = False, sim: bool = False) -> Tuple[str, Collection]:
        """
        generic funciton to create a new database entry in a given collection
        """
        collection = self.get_collection(type_name, test, sim)
        result = collection.insert_one(data)
        self.logger.debug(f"Added new entry {result.inserted_id} to database collection {collection.name}")
        return result.inserted_id, collection

    def get_experiments_in_time_range(self, start_date: datetime = None, end_date: datetime = None,  query: dict = None):
        if query is None:
            query = {}
        if start_date is not None:
            query["meta.starttime"] = {'$gte': start_date}
        if end_date is not None:
            # If 'meta.starttime' key doesn't exist in the query yet, create it
            query.setdefault('meta.starttime', {})
            query['meta.starttime']['$lt'] = end_date
        return query

    def _get_entries_by_ids(self, ids: list, query: dict = None) -> dict:
        if query is None:
            query = {}
        object_ids = [ObjectId(id_str) for id_str in ids]
        query['_id'] = {'$in': object_ids}
        return query

    def get_entries_where_key_exists(self, key: str, query: dict = None):
        if query is None:
            query = {}
        query.setdefault(key, {})
        query[key]['$exists'] = True
        return query

    def get_entries_where_key_not_exists(self, key: str, query: dict = None):
        if query is None:
            query = {}
        query.setdefault(key, {})
        query[key]['$exists'] = False
        return query

    def get_entries_at_station(self, stationName: str, query: dict = None):
        return self._get_entries_with_value("GlobalNavigation.goalStation", stationName, query=query)
    
    def get_sim_360_entry(self, objectId: int, runId: int = 0, test: bool = False):
        collection = self.get_collection("360", test=test)
        query = self._get_entries_with_value("objectId", objectId)
        query = self._get_entries_with_value("sim360_name", f"{objectId}_{runId}", query=query)
        num_results = collection.count_documents(query)
        if num_results == 0:
            print(f"Found 0 sim360 results for objectId {objectId} / run {runId}, won't return sim360 entry")
            return None
        elif num_results > 1:
            print(f"Found {num_results} sim360 results for objectId {objectId} / run {runId}, cannot return single entry")
            return None
        entry = collection.find_one(query)
        if entry["finished"] is False:
            print(f"sim360 entry is not yet finished, won't return entry")
            return None
        return entry

    def _get_entries_with_value(self, key: str, value, query: dict = None) -> dict:
        if query is None:
            query = {}
        query[key] = value
        return query

    def _get_entries_with_value_range(self, key: str, value: List, query: dict = None) -> dict:
        if query is None:
            query = {}
        query[key] = {'$gte': value[0], '$lte': value[1]}  # greater than or equal, lesser than or equal
        return query

    def get_entries_by_object_configuration(self, objectId: int, angle_range: List[int], side: str, conveyor: str = None, pathToDoe: str = None,
                                            sim: bool = False, test: bool = False, printResult: bool = False) -> Cursor:
        collection = self.get_collection(sim, test)
        query = self._get_entries_with_value("object.id", objectId)
        query = self._get_entries_with_value("object.side", side, query)
        if conveyor is not None:
            query = self._get_entries_with_value("object.conveyor", conveyor, query)
        query = self._get_entries_with_value_range("object.angle", angle_range, query)
        if pathToDoe is not None:
            df = self.load_doe(pathToDoe)
            ids = self.doe_get_ids(df=df, sim=sim)
            query = self._get_entries_by_ids(ids, query)
        num_results = collection.count_documents(query)
        results = collection.find(query)
        print(f"Found {num_results} matching results")
        if num_results == 1:
            for result in results:
                print(f"Configuration: {result['object']}")
                id_real = result["_id"]
                print(f"Id Real: {id_real}, folder_path: {self.get_result_folder_of_entry(id_real, sim=False, test=test)}")
                id_sim = self.get_sim_equivalent_entry(str(id_real), pathToDoe, test=test)
                print(f"Id Sim: {id_sim}, folder_path: {self.get_result_folder_of_entry(id_sim, sim=True, test=test)}")
                #pprint.pprint(result)
        return results

    def get_sim360_ids(self, objectId: int, test: bool = False):
        collection = self.get_collection("360", test=test)
        query = self._get_entries_with_value("objectId", objectId)
        query = self._get_entries_with_value("finished", True)
        query = self._get_entries_with_value("name", {"$regex": f"^{objectId}_.*"}, query)
        num_results = collection.count_documents(query)
        results = collection.find(query)
        ids = []
        for result in results:
            ids.append(result["_id"])
        print(f"Found {num_results} matching results")
        return ids

    # def get_entry_by_id(self, collection: Collection, id: str) -> dict:
    #     return collection.find_one({'_id': ObjectId(id)})

    def get_most_recent_entry(self, sim: bool = False, test: bool = False) -> dict:
        collection = self.get_collection(sim, test)
        return collection.find_one(sort=[('_id', -1)])

    def get_all_values(self, list_of_ids: list, field_name: str, collection_name: str = "Experiment", stationName: str = None):
        result_list = []
        collection = self.get_collection("Experiment", test=False, sim=False)
        query = self._get_entries_by_ids(list_of_ids)
        if stationName is not None:
            query = self.get_entries_at_station(stationName=stationName)
        query = self.get_entries_where_key_exists(field_name, query)
        projection = {field_name: 1, '_id': 0}  # Include only the specified field in the result
        if collection_name == "Experiment":
            result = collection.find(query, projection)
            for doc in result:
                result_list.append(deep_get(doc, field_name))
        return result_list

    def update_database_entry(self, collection: Collection,
                              target_id: str,
                              field_to_update: str,
                              new_value=None,
                              unset: bool = False,
                              append_array: bool = False,
                              increase_int: int = None,
                              upsert: bool = False) -> None:
        try:
            if append_array is True:
                result = collection.update_one({'_id': ObjectId(str(target_id))}, {'$push': {field_to_update: new_value}}, upsert=upsert)
            elif increase_int is not None:
                result = collection.update_one({'_id': ObjectId(str(target_id))}, {'$inc': {field_to_update: increase_int}}, upsert=upsert)
            elif unset is False:
                result = collection.update_one({'_id': ObjectId(str(target_id))}, {'$set': {field_to_update: new_value}}, upsert=upsert)
            else:
                result = collection.update_one({'_id': ObjectId(str(target_id))}, {'$unset': {field_to_update: 1}}, upsert=upsert)
            # Check if the update was successful
            #if result.matched_count > 0:
            #    print("Found matching field")
            #if result.modified_count > 0:
            #    print(f"Field '{field_to_update}' of _id {target_id} updated successfully.")
        except PyMongoError as e:
            print(f"Error updating database entry: {e}")

    def get_transfer_results(self, result_entry: dict) -> tuple:
        """
        Get the transfer results as a tuple (transferToStation, transferFromStation)
        Can be either True, False or None for each entry
        """
        toStationSuccess = None
        fromStationSuccess = None
        if "TransferToStation" in result_entry:
            toStationSuccess = result_entry["TransferToStation"]["success"]
            if "TransferFromStation" in result_entry:
                fromStationSuccess = result_entry["TransferFromStation"]["success"]
        return toStationSuccess, fromStationSuccess

    def get_transfer_success(self, result_entry: dict) -> int:
        """
        Get overall transfer success:
            0: unable to finish transfer (toStation and fromStation)
            1: succeded transfer (toStation and fromStation)
        """
        overall_result = 0
        transferToStation, transferFromStation = self.get_transfer_results(result_entry)
        if transferToStation is True and transferFromStation is True:
            overall_result = 1
        return overall_result

    def get_object_info(self, result_entry: dict) -> tuple:
        angle = None
        objectId = None
        side = None
        conveyor = None
        if result_entry.get('object', {}).get('id', {}) is not None:
            objectId = result_entry["object"]["id"]
        if result_entry.get('object', {}).get('angle', {}) is not None:
            angle = result_entry["object"]["angle"]
        if result_entry.get('object', {}).get('side', {}) is not None:
            side = result_entry["object"]["side"]
        if result_entry.get('object', {}).get('conveyor', {}) is not None:
            conveyor = result_entry["object"]["conveyor"]
        return objectId, side, angle, conveyor

    def get_sim_equivalent_entry(self, real_id: str, pathToDoe: str, test: bool = False) -> str:
        df = self.load_doe(pathToDoe)
        filtered_df = df[df['databaseEntry'] == real_id]
        if len(filtered_df) == 1:
            return filtered_df['simDatabaseEntry'].iloc[0]

    def get_result_folder_of_entry(self, target_id: str, sim: bool = False, test: bool = False):
        entry = self.get_entry_by_id(target_id, sim, test)
        if "VisionPreTransfer" in entry:
            try:
                file_path = entry["VisionPreTransfer"]["depth"]["filePath"]
                directory_path = os.path.dirname(file_path)
                # print(f"Result folder of entry {target_id}: {directory_path}")
                return directory_path
            except KeyError:
                print("Failed to get result folder based on depth vision pre transfer path")
        else:
            return None

    def convert_to_bson(self, dict: dict):
        bson_data = BSON.encode(dict)
        print(bson_data)

    def convert_to_json(self, dict: dict):
        json_data = json.dumps(dict, default=json_util.default)
        return json_data


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    db = DatabaseClient("agiprobot.ifl.kit.edu", "jfk", "jfk")
    # results = db.get_most_recent_entry(test=False)
    # print last item
    #item = db.experimentsTest.find().limit(1).sort([('$natural',-1)])
    # experiments =  db.experimentsTest.find()
    #for item_to_print in item:
    #    pprint.pprint(item_to_print)
    #start_date = datetime(2023, 12, 18, 10, 50)
    #end_date = datetime(2023, 12, 18, 11, 0)
    #query = db.get_experiments_in_time_range(start_date=None, end_date=end_date, query=None)
    #query = db.get_entries_where_key_exists('TransferFromStation', query=query)
    #query = db.get_entries_at_station("StationC", query=query)
    #results = db.experimentsTest.find(query)
    # result = db.experiment.find()

    #for result in results:
    # pprint.pprint(results)
