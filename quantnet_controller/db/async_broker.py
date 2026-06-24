"""
Async Broker — mirrors broker.py using AsyncCollection (pymongo native async).
"""

import os
import abc
from functools import wraps
from quantnet_controller.utils.util import import_classes_from_package
from quantnet_controller.db.nosql.collection.async_collection import AsyncCollection
from quantnet_controller.db.broker import (
    DATABASE_SECTION, BrokerType, check_database_type,
)

__ASYNC_BROKER = None
__ASYNC_BROKER_CONFIG = None


def init_async_broker_config(config):
    global __ASYNC_BROKER_CONFIG
    __ASYNC_BROKER_CONFIG = config


class AsyncBroker(object, metaclass=abc.ABCMeta):
    def _get_hndl(self, model):
        return self.classes[model]() if self.classes.get(model) else self.default(model)

    async def add(self, model, data, **kwargs):
        return await self._get_hndl(model).add(data, **kwargs)

    async def get(self, model, id, **kwargs):
        return await self._get_hndl(model).get(id, **kwargs)

    async def find(self, model, **kwargs):
        return await self._get_hndl(model).find(**kwargs)

    async def update(self, model, id, key, value, **kwargs):
        return await self._get_hndl(model).update(id, key, value, **kwargs)

    async def upsert(self, model, id, *args, **kwargs):
        return await self._get_hndl(model).upsert(id, *args, **kwargs)

    async def delete(self, model, id, **kwargs):
        return await self._get_hndl(model).delete(id, **kwargs)

    async def exist(self, model, id, **kwargs):
        return await self._get_hndl(model).exist(id, **kwargs)

    async def drop(self, model, **kwargs):
        return await self._get_hndl(model).drop(**kwargs)

    async def drop_database(self, model, **kwargs):
        return await self._get_hndl(model).drop_database(**kwargs)


class AsyncMongoBroker(AsyncBroker):
    def __init__(self, config=None, **kwargs):
        current_file_directory = os.path.dirname(os.path.abspath(__file__))
        # Read collection config from sync classes — they are the single source of truth
        # for _capped, _history, _size. No parallel async directory needed.
        self._sync_classes = import_classes_from_package(f"{current_file_directory}/nosql/collection")

    def _get_hndl(self, model):
        """Build an AsyncCollection stamped with config from the matching sync class."""
        sync_cls = self._sync_classes.get(model)
        ac = AsyncCollection(model)
        if sync_cls:
            ac._capped = getattr(sync_cls, '_capped', False)
            ac._history = getattr(sync_cls, '_history', False)
            ac._size = getattr(sync_cls, '_size', None)
        return ac


def async_broker(func):
    """Async version of the @broker decorator. Lazily initializes AsyncMongoBroker."""
    @wraps(func)
    async def wrapper(*args, broker=None, **kwargs):
        global __ASYNC_BROKER
        if __ASYNC_BROKER is None:
            url = __ASYNC_BROKER_CONFIG.get(
                DATABASE_SECTION, 'default',
                default="mongodb://localhost",
                check_config_table=False,
            ) if __ASYNC_BROKER_CONFIG else "mongodb://localhost"
            broker_type = check_database_type(url)
            if broker_type == BrokerType.MONGO:
                broker = AsyncMongoBroker(config=__ASYNC_BROKER_CONFIG)
            else:
                raise Exception(f"Async broker only supports MongoDB, got {url}")
            __ASYNC_BROKER = broker
        else:
            broker = __ASYNC_BROKER

        return await func(*args, broker=broker, **kwargs)

    return wrapper
