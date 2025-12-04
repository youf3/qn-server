# -*- coding: utf-8 -*-

import logging
import json

log = logging.getLogger(__name__)


class Simulator():
    def __init__(self, config, rpcclient, rtype="simulations", key="agentId", **kwargs):
        self._calibration_tasks = []
        self._rpcclient = rpcclient
        self._rpc_topic_prefix = config.rpc_client_topic

    async def simulate(self, request, timeout=50.0):
        name = request.payload.parameters.get("name")
        agent = request.payload.parameters.get("src")
        log.info(f"Sending Simulation request to {self._rpc_topic_prefix}/{agent} ")
        generationResp = self._rpcclient.call(
            "simulation.simulate",
            {"name": name,
             "params": {"purpose_id": 4, "number": 4}},
            topic=f"{self._rpc_topic_prefix}/{agent}",
            timeout=timeout
        )
        generationResp = json.loads(await generationResp)
        return generationResp
