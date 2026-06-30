"""
Async Collection — mirrors Collection using AsyncDBLayer (pymongo native async).
"""

from functools import wraps
from quantnet_controller.db.nosql.async_db import AsyncDBLoader

_ASYNC_DATABASE = None
_ASYNC_COLLECTION_CONFIG = None


def init_async_collection_config(config):
    global _ASYNC_COLLECTION_CONFIG
    _ASYNC_COLLECTION_CONFIG = config


class AsyncCollection:
    _keyname = "_id"
    _capped = False
    _history = False
    _size = None

    def __init__(self, model="default"):
        self._collection_name = model if model else "default"

    def async_layer(func):
        """Decorator that lazily initializes the AsyncDBLoader and injects an AsyncDBLayer."""
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            key = AsyncCollection._keyname
            global _ASYNC_DATABASE
            if _ASYNC_DATABASE is None:
                _ASYNC_DATABASE = AsyncDBLoader(config=_ASYNC_COLLECTION_CONFIG)
            layer = await _ASYNC_DATABASE.get_db_layer(
                self._collection_name, key,
                capped=self._capped, history=self._history, size=self._size,
            )
            return await func(self, *args, **kwargs, layer=layer)
        return wrapper

    @async_layer
    async def add(self, data, layer=None, **kwargs):
        if not isinstance(data, dict):
            raise Exception(f"Type error: {data} is not dict")
        result = await layer.insert(data)
        if isinstance(result, list):
            if len(result) == 1 and result[0].upserted_id:
                return data
            return None
        # InsertManyResult
        return data if result.inserted_ids else None

    @async_layer
    async def find(self, layer=None, **kwargs):
        q = kwargs.pop("filter", {})
        return await layer.find(q, **kwargs)

    @async_layer
    async def get(self, id, layer=None, **kwargs):
        if isinstance(id, dict):
            filter = id
        elif isinstance(id, str):
            filter = {self._keyname: id}
        else:
            raise Exception(f"{id} must be either dict or str")
        return await layer.find_one(filter)

    @async_layer
    async def update(self, id, key, value, layer=None):
        filter = {self._keyname: id} if not isinstance(id, dict) else id
        result = await layer.update(filter, {key: value})
        return True if result.modified_count else False

    @async_layer
    async def upsert(self, id, *args, layer=None, **kwargs):
        def combine(*args):
            combined_dict = {}
            for d in args:
                combined_dict.update(d)
            return combined_dict

        data = combine(*args) if args else {}
        if not data:
            return

        filter = {self._keyname: id} if not isinstance(id, dict) else id

        if await layer.find_one(filter):
            ret = await layer.update(filter, data)
            return True if ret.modified_count else False
        else:
            return await self.add(data)

    @async_layer
    async def delete(self, id, layer=None):
        filter = {self._keyname: id} if not isinstance(id, dict) else id
        result = await layer.remove(filter)
        return result.deleted_count

    @async_layer
    async def exist(self, id, layer=None, **kwargs):
        filter = {self._keyname: id} if not isinstance(id, dict) else id
        return True if await layer.find_one(filter) else False

    @async_layer
    async def count(self, layer=None, **kwargs):
        q = kwargs.pop("filter", {})
        return await layer.count(q, **kwargs)

    @async_layer
    async def create_index(self, keys, layer=None, **kwargs):
        return await layer.create_index(keys, **kwargs)

    @async_layer
    async def drop(self, layer=None, **kwargs):
        await layer.drop(**kwargs)

    @async_layer
    async def drop_database(self, layer=None, **kwargs):
        await _ASYNC_DATABASE.drop_database(**kwargs)
