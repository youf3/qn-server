"""
User
"""

from quantnet_controller.db.nosql.collection import Collection


class User(Collection):
    def __init__(self):
        self._collection_name = "User"
