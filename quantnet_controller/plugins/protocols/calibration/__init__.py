import time
import logging
from quantnet_controller.common.plugin import ProtocolPlugin, PluginType
from quantnet_controller.common.request import RequestManager, RequestType, RequestParameter
from quantnet_mq import Code
from quantnet_mq.schema.models import (
    agentCalibrationResponse,
    Status as responseStatus,
)
from calibrator import Calibrator

logger = logging.getLogger(__name__)


class CalibrationProtocol(ProtocolPlugin):
    def __init__(self, context):
        super().__init__("calibration", PluginType.PROTOCOL, context)
        self._client_commands = [
            ("calibration.srcInit", None, "quantnet_mq.schema.models.calibration.srcInit"),
            ("calibration.dstInit", None, "quantnet_mq.schema.models.calibration.dstInit"),
            ("calibration.generation", None, "quantnet_mq.schema.models.calibration.generation"),
            ("calibration.calibration", None, "quantnet_mq.schema.models.calibration.calibration"),
            ("calibration.cleanUp", None, "quantnet_mq.schema.models.calibration.cleanUp"),
        ]
        self._server_commands = [
            ("calibrate", self.handle_calibration, "quantnet_mq.schema.models.agentCalibration"),
        ]
        self._msg_commands = list()
        self.ctx = context
        self._calibrator = Calibrator(context.config, context.rpcclient, context.msgclient)
        self.request_manager = RequestManager(
            context, plugin_schema=agentCalibrationResponse, request_type=RequestType.CALIBRATION
        )

    def initialize(self):
        pass

    def destroy(self):
        pass

    def reset(self):
        pass

    async def handle_calibration(self, request):
        """handle calibration"""
        logger.info(f"Received calibration: {request.serialize()}")
        rc = 0
        if request.payload.type == "calibrate":
            parameters = RequestParameter(
                exp_name="Calibration",
                path=[request.payload.parameters["src"]._value, request.payload.parameters["dst"]._value],
                exp_params=request.payload.parameters.as_dict(),
            )
            req = self.request_manager.new_request(
                payload=request.payload, parameters=parameters)
            rc = await self.request_manager.schedule(req, blocking=False)
            return agentCalibrationResponse(
                status=responseStatus(code=Code.OK, value=Code(Code.OK).name),
                message=agentCalibrationResponse.__name__,
                calibrations=[
                    {
                        "phase": "Initializing",
                        "type": request.payload.parameters["type"],
                        "src": request.payload.parameters["src"],
                        "dst": request.payload.parameters["dst"],
                        "power": request.payload.parameters["power"],
                        "light": request.payload.parameters["cal_light"],
                        "start_ts": time.time(),
                        "id": req.id,
                    }
                ],
            )
        elif request.payload.type == "get":
            params = request.payload.parameters
            exp_id = getattr(params.get("id"), "_value", params.get("id")) if params else None
            src = getattr(params.get("src"), "_value", params.get("src")) if params else None
            dst = getattr(params.get("dst"), "_value", params.get("dst")) if params else None
            if exp_id:
                calib = await self.request_manager.get_request(exp_id, raw=True)
                calibs = [calib.to_dict() if hasattr(calib, "to_dict") else calib] if calib else []
            else:
                query_filter = {"type": RequestType.CALIBRATION.value}
                if src:
                    query_filter["parameters.exp_params.src"] = src
                if dst:
                    query_filter["parameters.exp_params.dst"] = dst
                calibs = await self.request_manager.find_requests(
                    filter=query_filter, raw=True, sort=[("created_at", -1)]
                )
            return agentCalibrationResponse(status=responseStatus(code=rc, value=Code(rc).name), calibrations=calibs)
        elif request.payload.type == "getLast":
            params = request.payload.parameters
            exp_id = getattr(params.get("id"), "_value", params.get("id")) if params else None
            src = getattr(params.get("src"), "_value", params.get("src")) if params else None
            dst = getattr(params.get("dst"), "_value", params.get("dst")) if params else None
            query_filter = {"type": RequestType.CALIBRATION.value}
            if exp_id:
                query_filter["id"] = exp_id
            if src:
                query_filter["parameters.exp_params.src"] = src
            if dst:
                query_filter["parameters.exp_params.dst"] = dst
            calibs = await self.request_manager.find_requests(
                filter=query_filter, raw=True, sort=[("created_at", -1)]
            )
            if calibs:
                with_result = [
                    c
                    for c in calibs
                    if isinstance(c, dict)
                    and c.get("result")
                    and not (set(c.get("result", {}).keys()) <= {"errors"})
                ]
                calibs = [with_result[0] if with_result else calibs[0]]
            return agentCalibrationResponse(
                status=responseStatus(code=rc, value=Code(rc).name), calibrations=calibs
            )
        else:
            raise Exception(f"unknown calibration type {request.payload.type}")
