# -*- coding: utf-8 -*-

import logging
import asyncio
import json
import time
from quantnet_controller.core import AbstractDatabase as DB, DBmodel

log = logging.getLogger(__name__)


class Calibrator:
    def __init__(self, config, rpcclient, msgclient, rtype="calibrations", key="agentId", **kwargs):
        self._calibration_tasks = []
        self._rpcclient = rpcclient
        self._msgclient = msgclient
        self._rpc_topic_prefix = config.rpc_client_topic
        self._handler = DB().handler(DBmodel.Calibration)

    async def start_calibration(self, parameters):
        cal_id = parameters.get("id")
        cal_type = parameters.get("type")
        src = parameters.get("src")
        dst = parameters.get("dst")
        power = parameters.get("power")
        light = parameters.get("cal_light")
        msgtopic = f"calibration-{cal_id}"

        """Thread to execute calibration process"""
        try:
            data = {
                "id": cal_id,
                "type": cal_type,
                "src": src,
                "dst": dst,
                "power": power,
                "light": light,
                "phase": "Initialized",
                "start_ts": time.time(),
                "end_ts": 0
            }
            self._handler.add(data)

            srcresp, dstresp = await self.init(src, dst, power)
            if srcresp["status"]["code"] != 0 or dstresp["status"]["code"] != 0:
                raise Exception(
                    f"{Calibrator.start_calibration.__qualname__}"
                    f"init failed: src: {srcresp['status']}, dst: {dstresp['status']}"
                )

            self._handler.update({"id": cal_id}, key="phase", value="Calibrating")
            await self._msgclient.publish(msgtopic, {"id": cal_id, "phase": "Calibrating"})
            srcresp, dstresp = await self.calibrate(src, dst, light)
            if srcresp["status"]["code"] != 0 or dstresp["status"]["code"] != 0:
                raise Exception(
                    f"{Calibrator.start_calibration.__qualname__}"
                    f"calibrate failed: src: {srcresp['status']}, dst: {dstresp['status']}"
                )

            self._handler.update({"id": cal_id}, key="phase", value="Cleanup")
            self._handler.update({"id": cal_id}, key="end_ts", value=time.time())
            await self._msgclient.publish(msgtopic, {"id": cal_id, "phase": "Cleanup"})
            srcresp, dstresp = await self.cleanup(src, dst)
            if srcresp["status"]["code"] != 0 or dstresp["status"]["code"] != 0:
                raise Exception(
                    f"{Calibrator.start_calibration.__qualname__}"
                    f"cleanup failed: src: {srcresp['status']}, dst: {dstresp['status']}"
                )

            self._handler.update({"id": cal_id}, key="phase", value="Done")
            self._handler.update({"id": cal_id}, key="end_ts", value=time.time())
            await self._msgclient.publish(msgtopic, {"id": cal_id, "phase": "Done"})
        except TimeoutError:
            log.error(f"{Calibrator.start_calibration.__qualname__}: calibration requests timeout.")
            self._handler.update(DBmodel.Calibration, cal_id, key="phase", value="Failed")
            await self._msgclient.publish(msgtopic, {"id": cal_id, "phase": "Failed"})
        except Exception as e:
            log.error(f"{Calibrator.start_calibration.__qualname__}: calibration failed: {e}")
            self._handler.update({"id": cal_id}, key="phase", value="Failed")
            await self._msgclient.publish(msgtopic, {"id": cal_id, "phase": "Failed"})
        finally:
            pass
        return

    async def startCalibration(self, params):
        """Start a new calibration process"""
        log.info("starting calibration request")

        try:
            task = asyncio.create_task(self.start_calibration(params.as_dict()))
            self._calibration_tasks.append(task)
        except Exception as e:
            raise Exception(f"calibraton failed between {params['src']} and {params['dst']}: {e.args}")

    async def getCalibration(self, request, last=False):
        log.info("processing get calibration request")

        if last:
            """get last calibration"""
            res = self._handler.find({}, limit=1, sort={"created_at": -1})
            return res[0] if res else None
        elif request.payload.parameters.get("id"):
            """return existing calibrations with given id"""
            cal_id = str(request.payload.parameters["id"])
            if self._handler.exist({"id": cal_id}):
                cal = self._handler.get({"id": cal_id})
                return {
                    "phase": cal["phase"],
                    "src": cal["src"],
                    "dst": cal["dst"],
                    "power": cal["power"],
                    "light": cal["light"],
                    "cal_id": cal_id,
                    "start_ts": cal["start_ts"],
                    "end_ts": cal["start_ts"]
                }
            else:
                raise Exception(f"calibration not found by id: {cal_id}")
        elif request.payload.parameters.get("src") and request.payload.parameters.get("dst"):
            """return existing calibrations with given src and dst"""
            log.error("Unimplemented calibration query")
            return dict()
        else:
            return self._handler.find()

    async def init(self, src, dst, power, timeout=50.0):
        log.info("Sending Calibration initialization")
        rpc_topic_prefix = self._rpc_topic_prefix
        init_tasks = []

        init_tasks.append(
            self._rpcclient.call(
                "calibration.srcInit", {"power": power}, topic=f"{rpc_topic_prefix}/{src}", timeout=timeout
            )
        )
        init_tasks.append(
            self._rpcclient.call("calibration.dstInit", None, topic=f"{rpc_topic_prefix}/{dst}", timeout=timeout)
        )
        result = await asyncio.gather(*init_tasks)
        return json.loads(result[0]), json.loads(result[1])

    async def calibrate(self, src, dst, cal_light, timeout=50.0):
        log.info("Sending Calibration")
        rpc_topic_prefix = self._rpc_topic_prefix
        generationResp = self._rpcclient.call(
            "calibration.generation", {"cal_light": cal_light}, topic=f"{rpc_topic_prefix}/{src}", timeout=timeout
        )
        generationResp = json.loads(await generationResp)
        calibrateResp = self._rpcclient.call(
            "calibration.calibration", {"cal_light": cal_light}, topic=f"{rpc_topic_prefix}/{dst}", timeout=timeout
        )
        calibrateResp = json.loads(await calibrateResp)
        return generationResp, calibrateResp

    async def cleanup(self, src, dst, timeout=50.0):
        rpc_topic_prefix = self._rpc_topic_prefix
        log.info("Starting Calibration cleanup")
        cleanup_tasks = []
        cleanup_tasks.append(
            self._rpcclient.call("calibration.cleanUp", None, topic=f"{rpc_topic_prefix}/{src}", timeout=timeout)
        )
        cleanup_tasks.append(
            self._rpcclient.call("calibration.cleanUp", None, topic=f"{rpc_topic_prefix}/{dst}", timeout=timeout)
        )

        result = await asyncio.gather(*cleanup_tasks)
        return json.loads(result[0]), json.loads(result[1])
