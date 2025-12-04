# -*- coding: utf-8 -*-

import logging
import json
from quantnet_mq import Code
import asyncio

log = logging.getLogger(__name__)


class ScheduleManager():
    def __init__(self, rpcclient, rtype="function", key="agentId", **kwargs):
        # self._function_tasks = []
        self._rpcclient = rpcclient

    async def get_schedule(self, agent_id, param, timeout=5.0):
        log.info(f"getting schedule from {agent_id}")
        submitResp = await self._rpcclient.call(
            "scheduler.getSchedule", param, topic=f"rpc/{agent_id}", timeout=timeout
        )
        submitResp = json.loads(submitResp)
        return submitResp

    async def get_timeslots(self, agent_ids, param, timeout=5):
        log.info(f"Fetching timeslots from agents {agent_ids}")
        slots = {}
        timeslot_futures = {}
        for agent_id in agent_ids:
            fut = self.get_schedule(agent_id, param, timeout=timeout)
            timeslot_futures[agent_id] = fut

        try:
            results = await asyncio.gather(*timeslot_futures.values(), return_exceptions=True)

            for agent_id, result in zip(timeslot_futures.keys(), results):
                if isinstance(result, Exception):
                    log.error(f"Failed to get timeslot from agent {agent_id}: {result}")
                    raise Exception("Failed to get timeslot from agents")

                if result is not None and result["status"]["code"] == Code.OK.value:
                    slots[agent_id] = bin(int(result["payload"]["timeslots"], 16))[2:].zfill(param["numSlots"])
                else:
                    log.error(f"Failed to get timeslot from agent {agent_id}")
                    raise Exception("Failed to get timeslot from agents")

        except Exception as e:
            log.error(f"Exception occurred while gathering timeslots: {e}")
            raise

        return slots

    async def _call_cancel_exp(self, agent_id, exp_id, timeout=5.0):
        log.info(f"Submit cancel schedule from {agent_id}")
        try:
            submitResp = await self._rpcclient.call(
                "experiment.cancel", {"exp_id": exp_id}, topic=f"rpc/{agent_id}", timeout=timeout
            )
            submitResp = json.loads(submitResp)
            return submitResp
        except TimeoutError:
            return None

    async def cancel_tasks(self, exp_id, agent_Ids, timeout=5):
        log.info(f"Cancelling experiment {exp_id}")
        # Experiment.update(exp_id, key="phase", value="cancelling")
        aws = {}

        for agent_id in agent_Ids:
            aws[agent_id] = self._call_cancel_exp(agent_id, exp_id, timeout=timeout)

        try:
            results = await asyncio.gather(*aws.values(), return_exceptions=True)

            for agent_id, result in zip(aws.keys(), results):
                if result is not None and result["status"]["code"] == Code.OK.value:
                    continue
                else:
                    log.error(f"Failed to cancel experiment in agent {agent_id}: {result}")

        except Exception as e:
            log.error(f"Exception occurred while canceling experiments: {e}")
            raise

        # Experiment.update(exp_id, key="phase", value="Failed to cancel")
