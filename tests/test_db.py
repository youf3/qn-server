#!/usr/bin/env python3

import pytest
import os
import json
from quantnet_controller.common.utils import generate_uuid
from quantnet_controller.core import DBmodel
from . import QuantnetTest


class TestDB(QuantnetTest):

    @pytest.fixture(autouse=True)
    def setup(self):
        super().setup()

    @pytest.fixture
    def nodes(self):
        yield self.add_nodes()
        self.db.drop(DBmodel.Node)

    def test_list_node(self, nodes):
        try:
            nodes = self.db.find(DBmodel.Node)
        except Exception:
            assert (False)
        assert (len(nodes) == len(self._node_configs))

    def test_get_node(self):
        try:
            fname = os.path.join(self._node_path, "conf_lbnl-q.json")
            fp = open(fname)
            data = json.load(fp)
            result = self.db.add(DBmodel.Node, data)
            assert (result['systemSettings']['ID'] == "LBNL-Q")
        except Exception:
            assert (False)

        try:
            node = self.db.get(DBmodel.Node, {"systemSettings.ID": "LBNL-Q"})
            assert (node['systemSettings']['ID'] == "LBNL-Q")
        except Exception:
            assert (False)

        self.db.drop(DBmodel.Node)

    def test_add_calibration(self):
        id = generate_uuid()
        data = {"id": id,
                "src": "agent1",
                "dst": "agent2",
                "power": 0.1,
                "light": "H"}

        result = self.db.add(DBmodel.Calibration, data)
        assert (result.get("id") == id)
        self.db.drop(DBmodel.Calibration)

    def test_list_calibration(self):
        data = self.db.find(DBmodel.Calibration)
        assert (data == [])

    def test_get_and_drop_calibration(self):
        id = generate_uuid()
        data = {"id": id,
                "src": "agent1",
                "dst": "agent2",
                "power": 0.1,
                "light": "H"}
        self.db.add(DBmodel.Calibration, data)

        data = self.db.get(DBmodel.Calibration, {"id": id})
        assert (data.get("id") == id)
        self.db.drop(DBmodel.Calibration)

        data = self.db.get(DBmodel.Calibration, {"id": id})
        assert (data is None)

    def test_update_calibration(self):
        id = generate_uuid()
        data = {"id": id,
                "src": "agent1",
                "dst": "agent2",
                "power": 0.1,
                "light": "H"}
        data = self.db.add(DBmodel.Calibration, data)
        assert (data.get("id") == id)

        res = self.db.update(DBmodel.Calibration, {"id": id}, "src", "agent3")
        assert (res is True)

        data = self.db.get(DBmodel.Calibration, {"id": id})
        assert (data.get("src") == "agent3")

        self.db.drop(DBmodel.Calibration)

    def test_del_calibration(self):
        id = generate_uuid()
        data = {"id": id,
                "src": "agent1",
                "dst": "agent2",
                "power": 0.1,
                "light": "H"}
        self.db.add(DBmodel.Calibration, data)

        result = self.db.delete(DBmodel.Calibration, {"id": id})
        assert (result == 1)
        self.db.drop(DBmodel.Calibration)

    def test_exist_calibration(self):
        id = generate_uuid()
        data = {"id": id,
                "src": "agent1",
                "dst": "agent2",
                "power": 0.1,
                "light": "H"}
        self.db.add(DBmodel.Calibration, data)

        result = self.db.exist(DBmodel.Calibration, {"id": id})
        assert (result is True)
        self.db.drop(DBmodel.Calibration)

    def test_DB_default_handler(self):
        handler = self.db.handler()

        name = generate_uuid()
        data = {"name": name,
                "remote": "alice",
                "phase": "start",
                "reason": ""}
        result = handler.add(data)

        id = {"name": result.get("name")}

        result = handler.get(id)
        assert (result.get("name") == name)

        result = handler.get({"remote": "alice"})
        assert (result.get("phase") == "start")

        result = handler.find()
        assert (result[0].get("remote") == "alice")

        result = handler.exist(id)
        assert (result is True)

        result = handler.exist({"remote": "alice"})
        assert (result is True)

        result = handler.exist({"remote": "charles"})
        assert (result is False)

        # update
        res = handler.update(id, key="reason", value="charliecat")
        assert (res is True)
        result = handler.get(id)
        assert (result.get("name") == name)

        # upsert
        res = handler.upsert(id, {"reason": "bobcat", "phase": "end"})
        assert (res is True)
        result = handler.get(id)
        assert (result.get("remote") == "alice")

        res = handler.upsert({"remote": "alice"}, {"name": "alicecat"})
        assert (res is True)
        result = handler.get({"name": "alicecat"})
        assert (result.get("name") == "alicecat")

        # deletes
        result = handler.delete(id)
        assert (result == 0)

        result = handler.delete({"name": "alicecat"})
        assert (result == 1)

        result = handler.get(id)
        assert (result is None)

        result = handler.find()
        assert (result == [])

        handler.upsert({"remote": "alice"}, {"name": "alicecat"})
        result = handler.get({"name": "alicecat"})
        assert (result.get("name") == "alicecat")

        result = handler.drop()
        assert (result is None)

        result = handler.find()
        assert (result == [])

        del handler
