from quantnet_controller.common.plugin import SchedulingPlugin, PluginType
from quantnet_controller.plugins.scheduling.scheduler import Scheduler
import logging
from schedule_manager import ScheduleManager


log = logging.getLogger(__name__)


class BatchScheduler(SchedulingPlugin):
    def __init__(self, context):
        super().__init__("scheduler", PluginType.SCHEDULING, context=context)
        self._client_commands = [
             ("scheduler.getSchedule", None, "quantnet_mq.schema.models.scheduler.getSchedule"),
        ]
        self._schedule_manager = ScheduleManager(context.rpcclient)

    def initialize(self):
        pass

    def destroy(self):
        pass

    def reset(self):
        pass

    def start(self):
        self._s = Scheduler()
        self._s.start()

    async def schedule(self, func, args, repeat: int = 1, interval: int = 6, duration: int = 5, blocking: bool = True):
        return await self._s.schedule(func, args, repeat, interval, duration, blocking)

    async def get_schedule(self, agent_id, param, timeout=5.0):
        return await self._schedule_manager.get_schedule(agent_id, param, timeout=timeout)

    async def get_timeslots(self, agent_ids, param, timeout=5.0):
        return await self._schedule_manager.get_timeslots(agent_ids, param, timeout=timeout)

    async def cancel_tasks(self, exp_id, agent_Ids, timeout=5):
        await self._schedule_manager.cancel_tasks(exp_id, agent_Ids, timeout=timeout)
