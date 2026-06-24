"""
Async DB Layer — mirrors db.py using pymongo's native async API (pymongo >= 4.9).
"""

import time
import logging
import sys

from bson.objectid import ObjectId
from pymongo import AsyncMongoClient
from quantnet_controller.common.utils import get_uri_path


class AsyncDBLayer:
    """
    Async wrapper for MongoDB collections.

    Same semantics as DBLayer but all I/O methods are async def.
    """

    def __init__(self, client, collection_name, capped=False, Id="id", timestamp="ts", *, history=False):
        self.log = logging.getLogger(__name__)
        self.Id = Id
        self.timestamp = timestamp
        self.history, self.capped = history, capped
        self._collection_name = collection_name
        self._client = client

    @property
    def collection(self):
        """Returns a reference to the mongodb collection."""
        return self._client[self._collection_name]

    async def find_one(self, query={}, **kwargs):
        self.log.debug(f"Find one for collection: [{self._collection_name}]")
        fields = kwargs.pop("fields", {})
        fields["_id"] = 0
        result = await self.collection.find_one(query, projection=fields, **kwargs)
        return result

    async def count(self, query={}, **kwargs):
        skip = kwargs.get("skip", 0)
        if "limit" in kwargs:
            return await self.collection.count_documents(query, skip=skip, limit=kwargs["limit"])
        return await self.collection.count_documents(query, skip=skip)

    async def find(self, query={}, **kwargs):
        """Finds one or more elements in the collection."""
        self.log.debug(f"Find for collection: [{self._collection_name}]")
        fields = kwargs.pop("fields", {})
        fields["_id"] = 0
        cursor = self.collection.find(query, fields, **kwargs)
        return await cursor.to_list()

    def _insert_id(self, data):
        if "_id" not in data and not self.capped:
            res_id = data.get(self.Id, str(ObjectId()))
            timestamp = data.get(self.timestamp, int(time.time() * 1e6))
            data["_id"] = f"{res_id}:{timestamp}" if self.history else res_id

    async def insert(self, data, summarize=True, **kwargs):
        """Inserts data to the collection."""
        self.log.debug(f"Insert for collection: [{self._collection_name}]")
        data = [data] if not isinstance(data, list) else data
        if not self.capped:
            for item in data:
                self._insert_id(item)

        if self.history:
            results = await self.collection.insert_many(data, **kwargs)
        else:
            results = []
            for item in data:
                rid = item.get(self.Id, str(ObjectId()))
                results.append(await self.collection.replace_one({self.Id: rid}, item, upsert=True))
        return results

    async def upsert(self, query, data):
        return await self.collection.replace_one(query, data, upsert=True)

    async def update(self, query, data, replace=False, multi=True, **kwargs):
        """Updates data found by query in the collection."""
        self.log.debug(f"Update for Collection: [{self._collection_name}]")
        if not replace:
            data = {"$set": data}
        if multi:
            results = await self.collection.update_many(query, data)
        else:
            results = await self.collection.find_one_and_update(query, data, upsert=False, **kwargs)
            for r in results:
                if isinstance(r, dict) and not r.get("updatedExisting", True):
                    raise LookupError("Resource ID does not exist")
        return results

    async def remove(self, query, callback=None, **kwargs):
        self.log.debug(f"Remove for collection: [{self._collection_name}]")
        results = await self.collection.delete_many(query)
        return results

    async def create_index(self, keys, **kwargs):
        """Create an index on the collection."""
        return await self.collection.create_index(keys, **kwargs)

    async def drop(self, **kwargs):
        await self.collection.drop()

    async def drop_database(self, **kwargs):
        await self._client.db.command("dropDatabase")


class AsyncDBLoader:
    def __init__(self, config=None, **kwargs):
        self.log = logging.getLogger(__name__)
        self._config = config
        self._dbname = kwargs.get("dbname", "quantnet")
        self._capped_collections = set()
        self._db = self._init()

    @property
    def db(self):
        return self._db

    def _init(self):
        """Initialize the async client. AsyncMongoClient constructor is sync;
        actual connection happens lazily on first operation."""
        try:
            url = self._config.get('database', 'default',
                                   default="mongodb://localhost",
                                   check_config_table=False) if self._config else "mongodb://localhost"
            path = get_uri_path(url)
            self._dbname = path if path else self._dbname
            self._conn = AsyncMongoClient(url)
        except Exception as exp:
            self.log.error(f"Failed to initialize async client - {exp}")
            sys.exit()
        self._db = self._conn[self._dbname]
        return self._db

    async def drop_database(self, **kwargs):
        await self._conn.drop_database(self._dbname)

    async def get_db_layer(self, collection_name, id_field_name, capped=False, history=False, size=None):
        if not collection_name:
            return None
        if capped and collection_name not in self._capped_collections:
            existing = await self.db.list_collection_names()
            if collection_name not in existing:
                cap_size = size or 10 * 1024 * 1024
                await self.db.create_collection(collection_name, capped=True, size=cap_size)
                self.log.info(f"Created capped collection '{collection_name}' (size={cap_size})")
            self._capped_collections.add(collection_name)
        db_layer = AsyncDBLayer(self.db, collection_name, capped, id_field_name, history=history)
        return db_layer
