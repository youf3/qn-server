import logging
import time
from datetime import datetime, timezone
from quantnet_controller.common.plugin import MonitoringPlugin, PluginType
from quantnet_mq.schema.models import monitor, Status, agentMonitorTaskResponse
from quantnet_mq import Code, EventType
from quantnet_controller.core import AsyncAbstractDatabase as DB, DBmodel

logger = logging.getLogger(__name__)


class Monitor(MonitoringPlugin):
    def __init__(self, context):
        super().__init__("monitor", PluginType.MONITORING, context)
        self._db = DB().handler(DBmodel.Monitor)
        self._state_db = DB().handler(DBmodel.MonitorState)
        self._node_db = DB().handler(DBmodel.Node)
        logger.info(f"Monitor plugin initialized with DB handler: {self._db}")
        self._msg_commands = [
            ("monitor", self.handle_resource_update)
        ]
        self._server_commands = [
            ("getTasks", self.handle_get_tasks, "quantnet_mq.schema.models.agentMonitorTask")
        ]

    async def handle_resource_update(self, request):
        logger.debug(f"Received resource update: {request}")
        try:
            obj = monitor.MonitorEvent.from_json(request)
            if obj.eventType == EventType.AGENT_HEARTBEAT:
                # Update last_seen timestamp on the node record
                agent_id = obj.rid
                now = time.time()
                await self._node_db.update(
                    {"systemSettings.ID": str(agent_id)}, "last_seen", now
                )
                logger.debug(f"Updated last_seen for node {agent_id} to {now}")
            elif obj.eventType == EventType.AGENT_STATE:
                # Capped collection — append-only, old entries auto-evicted
                await self._state_db.add(obj.as_dict())
                logger.info(f"{obj.rid} {obj.eventType} is updated : {obj.as_dict()}")
            elif obj.eventType == "link_state_update":
                self._handle_link_state_update(obj)
            else:
                doc = obj.as_dict()
                doc["created_at"] = datetime.now(timezone.utc)
                await self._db.add(doc)
                if obj.eventType == EventType.EXPERIMENT_RESULT:
                    logger.info(f"{obj.rid} {obj.eventType} is updated : {obj.value}")
                elif obj.eventType == EventType.AGENT_TASK_RESULT:
                    logger.info(f"{obj.rid} {obj.eventType} is updated : {obj.value}")
        except Exception as e:
            logger.warning(f"Failed to update resource : {e}")

    def _handle_link_state_update(self, event):
        """Handle link state update events from LinkAdjacencyManager."""
        logger.debug(
            "Link state update: %s → %s = %s",
            event.get("src_cid"), event.get("dst_cid"), event.get("state"),
        )
        try:
            # Fire-and-forget update to link_state collection (upsert)
            import asyncio
            asyncio.create_task(self._update_link_state(event))
        except Exception as e:
            logger.debug("Could not handle link_state_update: %s", e)

    async def _update_link_state(self, event):
        """Async update of link state, notifies resource manager."""
        try:
            link_db = DB().handler(DBmodel.LinkState)
            await link_db.replace_one(
                {"src_cid": str(event.get("src_cid")), "dst_cid": str(event.get("dst_cid"))},
                {
                    "src_cid": str(event.get("src_cid")),
                    "dst_cid": str(event.get("dst_cid")),
                    "state": str(event.get("state")),
                    "timestamp": str(event.get("timestamp")),
                },
                upsert=True,
            )
            # Notify resource manager if available (optional)
            if hasattr(self.context, "resource_mgr") and self.context.resource_mgr:
                self.context.resource_mgr.set_topo_updated()
        except Exception as e:
            logger.debug("Could not update link state: %s", e)

    async def handle_get_tasks(self, request):
        logger.debug(f"Received getTasks request: {request}")
        try:
            agent_id = request.payload.agent_id
            if agent_id:
                agent_id = str(agent_id).strip()

            filter = {"eventType": EventType.AGENT_TASK_RESULT}
            if agent_id:
                filter["rid"] = agent_id

            logger.info(f"Querying Monitor DB with filter: {filter}")
            results = await self._db.find(filter=filter)
            logger.info(f"Found {len(results)} results for filter {filter}")
            tasks = []
            for res in results:
                tasks.append({
                    "id": res["value"].get("exp_id"),
                    "type": "agentTask",
                    "status": {"code": 0, "value": "OK"},
                    "result": res["value"].get("result", {}),
                    "created_at": res["ts"],
                    "updated_at": res["ts"],
                    "phase": "completed",
                    "agentIds": [res["rid"]],
                    "expName": res["value"].get("name"),
                })
            return agentMonitorTaskResponse(
                status=Status(code=Code.OK.value, value=Code.OK.name), tasks=tasks
            )
        except Exception as e:
            logger.error(f"Failed to get tasks: {e}")
            return agentMonitorTaskResponse(
                status=Status(code=Code.INTERNAL.value, value=Code.INTERNAL.name, message=str(e)),
                tasks=[],
            )

    def initialize(self):
        pass

    def destroy(self):
        pass

    def reset(self):
        pass

    def start(self):
        from quantnet_controller.db.nosql.collection.monitor import Monitor as MonitorCollection
        MonitorCollection().ensure_indexes()
        logger.info("Monitor started and listening on /monitor topic")
