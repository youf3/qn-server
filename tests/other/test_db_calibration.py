#!/usr/bin/env python3

import os
import logging
import unittest
import json

from quantnet_controller.db.sqla.calibration import (
    add_calibration,
    calibration_exists,
    get_calibration,
    del_calibration,
    update_calibration,)
from quantnet_controller.common.utils import generate_uuid
# from quantnet_controller.db.sqla.constants import NodeStatus, NodeType

import quantnet_mq.schema

NODE_PATH = os.path.normpath(
    os.path.join(quantnet_mq.schema.__path__[0],
                 "examples/topology"))

NODES = ["conf_lbnl-switch.json",
         "conf_ucb-switch.json"]


class TestCalibration(unittest.IsolatedAsyncioTestCase):
    logger = logging.getLogger(__name__)
    log_format = \
        '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(handlers=[logging.StreamHandler()], format=log_format, force=True)

    async def test_add_calibration(self):

        id = generate_uuid()
        add_calibration(id=id,
                        src="agent1",
                        dst="agent2",
                        power=0.1,
                        light="H")
        assert (calibration_exists(id))

        calibration = get_calibration(id)
        print(json.dumps(calibration, indent=4, sort_keys=False))

        del_calibration(id)

    async def test_update_calibration(self):
        id = generate_uuid()
        add_calibration(id=id,
                        src="agent1",
                        dst="agent2",
                        power=0.1)
        assert get_calibration(id)['phase'] == "init"
        update_calibration(id, key='phase', value="generation")
        assert get_calibration(id)['phase'] == "generation"
        update_calibration(id, key='phase', value="cleanup")
        assert get_calibration(id)['phase'] == "cleanup"
        del_calibration(id)
