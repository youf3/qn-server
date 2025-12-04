#!/usr/bin/env python3

import logging
import unittest
import json

from quantnet_controller.core.node import Node


class TestQnode(unittest.IsolatedAsyncioTestCase):
    logger = logging.getLogger(__name__)
    log_format = \
        '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
    logging.basicConfig(handlers=[logging.StreamHandler()], format=log_format, force=True)


    async def test_list(self):
        try:
            nodes = Node().list()
        except Exception as e:
            raise e
        for n in nodes:
            print(json.dumps(n, indent=4, sort_keys=False))

    async def test_get_node_by_ID(self):

        try:
            device = Node().get_node_by_ID(ID="LBNL-Q")
        except Exception as e:
            raise e
        print(json.dumps(device, indent=4, sort_keys=False))

        try:
            device = Node().get_node_by_ID(ID="UCB-Q")
        except Exception as e:
            raise e
        print(json.dumps(device, indent=4, sort_keys=False))

        try:
            device = Node().get_node_by_ID(ID="LBNL-M")
        except Exception as e:
            raise e
        print(json.dumps(device, indent=4, sort_keys=False))

        try:
            device = Node().get_node_by_ID(ID="UCB-M")
        except Exception as e:
            raise e
        print(json.dumps(device, indent=4, sort_keys=False))
        try:
            device = Node().get_node_by_ID(ID="LBNL-BSM")
        except Exception as e:
            raise e
        print(json.dumps(device, indent=4, sort_keys=False))
        try:
            device = Node().get_node_by_ID(ID="LBNL-SWITCH")
        except Exception as e:
            raise e
        print(json.dumps(device, indent=4, sort_keys=False))

        try:
            device = Node().get_node_by_ID(ID="UCB-SWITCH")
        except Exception as e:
            raise e
        print(json.dumps(device, indent=4, sort_keys=False))
