"""
Monitor — regular collection for experiment/task result events.

Stores experimentResult, agentTaskResult, agentTaskSchedulerPhase, and
agentTaskSchedulerTask events. Indexes are created at server startup via
ensure_indexes(). A TTL index on created_at expires documents after 30 days.
"""

import logging
from pymongo import ASCENDING, DESCENDING

from quantnet_controller.db.nosql.collection import Collection

logger = logging.getLogger(__name__)

_TTL_SECONDS = 30 * 24 * 3600  # 30 days


class Monitor(Collection):
    _indexes_created = False

    def __init__(self):
        self._collection_name = "Monitor"

    def ensure_indexes(self):
        """Create indexes if not already done for this process. Idempotent."""
        if Monitor._indexes_created:
            return
        try:
            # Covers get_exp_results and handle_get_tasks eventType-only prefix
            self.create_index(
                [("eventType", ASCENDING), ("value.exp_id", ASCENDING)],
                name="idx_eventType_value_exp_id",
            )
            # Covers handle_get_tasks with agent_id filter
            self.create_index(
                [("eventType", ASCENDING), ("rid", ASCENDING)],
                name="idx_eventType_rid",
            )
            # TTL — expires documents 30 days after created_at (datetime field)
            self.create_index(
                [("created_at", ASCENDING)],
                expireAfterSeconds=_TTL_SECONDS,
                name="idx_ttl_created_at",
            )
            Monitor._indexes_created = True
            logger.info("Monitor collection indexes created")
        except Exception as e:
            logger.warning(f"Failed to create Monitor indexes: {e}")
