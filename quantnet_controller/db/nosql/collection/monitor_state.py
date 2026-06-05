"""
MonitorState — capped collection for agent state events.

Stores only agentState events. Old entries are automatically evicted when
the collection reaches _size, keeping only the most recent history.
"""

from quantnet_controller.db.nosql.collection import Collection


class MonitorState(Collection):
    _capped = True
    _history = True
    _size = 10 * 1024 * 1024  # 10 MB

    def __init__(self):
        self._collection_name = "MonitorState"
