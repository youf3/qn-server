import logging
from quantnet_controller.common.plugin import ProtocolPlugin, PluginType
from quantnet_mq.schema.models import (
    agentSimulationResponse,
    Status as responseStatus,
)
from quantnet_controller.plugins.protocols.simulation.simulator import Simulator
from quantnet_controller.common.utils import generate_uuid

logger = logging.getLogger(__name__)


class SimulationPrtocol(ProtocolPlugin):
    def __init__(self, context):
        super().__init__("simulation", PluginType.PROTOCOL, context)
        self._client_commands = [
            ("simulation.simulate", None, "quantnet_mq.schema.models.simulation.simulation"),
            # ("simulation.collectdata", self.dummy_response, "quantnet_mq.schema.models.simulation.collectdata"),
        ]
        self._server_commands = [
            ("simulate", self.handle_simulation, "quantnet_mq.schema.models.agentSimulation"),
        ]
        self._msg_commands = list()
        self.ctx = context
        self._simulator = Simulator(context.config, context.rpcclient)

    def initialize(self):
        pass

    def destroy(self):
        pass

    def reset(self):
        pass

    async def handle_simulation(self, request):
        """handle simulation"""
        logger.info(f"Received simulation request: {request.serialize()}")
        rc = 0
        if request.payload.type == "simulate":
            simid = generate_uuid()  # You'll need to import this

            parameters = {
                "id": simid,
                "name": request.payload.parameters.get("name"),
                "src": request.payload.parameters.get("src"),
                "params": request.payload.parameters,
            }

            rc = await self.ctx.request_middleware.schedule(parameters, "Simulation")
            return agentSimulationResponse(status=responseStatus(code=rc.value, value=rc.name))
        else:
            raise Exception(f"unknown simulation type {request.payload.type}")
