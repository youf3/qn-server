import logging
import time
from datetime import datetime, timezone
from quantnet_controller.common.plugin import MonitoringPlugin, PluginType
from quantnet_mq.schema.models import monitor, Status, agentMonitorTaskResponse
from quantnet_mq import Code, EventType
from quantnet_controller.core import AsyncAbstractDatabase as AsyncDB, DBmodel

logger = logging.getLogger(__name__)


class Monitor(MonitoringPlugin):
    def __init__(self, context):
        super().__init__("monitor", PluginType.MONITORING, context)
        self._db = AsyncDB().handler(DBmodel.Monitor)
        self._state_db = AsyncDB().handler(DBmodel.MonitorState)
        self._node_db = AsyncDB().handler(DBmodel.Node)
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
            elif obj.eventType == "linkStateUpdate":
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
        value = event.value if hasattr(event, "value") else event
        if isinstance(value, str):
            import json as _json
            value = _json.loads(value)
        logger.info(
            "Link state update: %s:%s -> %s = %s",
            value.get("src_cid"), value.get("channel_id"),
            value.get("dst_cid"), value.get("state"),
        )
        try:
            import asyncio
            asyncio.create_task(self._update_link_state(value))
        except Exception as e:
            logger.debug("Could not handle link_state_update: %s", e)

    async def _update_link_state(self, value):
        """Async upsert of link state, notifies resource manager."""
        try:
            link_db = AsyncDB().handler(DBmodel.LinkState)
            src_cid = str(value.get("src_cid", ""))
            dst_cid = str(value.get("dst_cid", ""))
            channel_id = str(value.get("channel_id", ""))
            state = str(value.get("state", "DOWN"))
            timestamp = str(value.get("timestamp", ""))
            await link_db.upsert(
                {"src_cid": src_cid, "dst_cid": dst_cid, "channel_id": channel_id},
                {
                    "src_cid": src_cid,
                    "dst_cid": dst_cid,
                    "channel_id": channel_id,
                    "state": state,
                    "timestamp": timestamp,
                },
            )
            # Notify resource manager to invalidate topology cache
            if hasattr(self._context, "rm") and self._context.rm:
                self._context.rm.set_topo_updated()
            # Notify routing plugin to rebuild its cached graph
            # Only refresh on states that affect routing (skip INIT which is frequent)
            if state in ("CONTROL_UP", "QUANTUM_UP", "DOWN"):
                if hasattr(self._context, "router") and self._context.router:
                    try:
                        self._context.router.refresh()
                    except Exception as e:
                        logger.debug("Could not notify router of topology change: %s", e)
        except Exception as e:
            logger.warning("Could not update link state in DB: %s", e)

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
