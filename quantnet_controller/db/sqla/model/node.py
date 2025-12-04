"""
Node
"""

import json
from sqlalchemy.orm import exc
from quantnet_mq.schema.models import QNode, MNode, BSMNode, OpticalSwitch, QRepeater
from quantnet_controller.common.utils import generate_uuid
from quantnet_controller.common import exception
from quantnet_controller.db.sqla import models
from quantnet_controller.db.sqla.constants import NodeType
from quantnet_controller.db.sqla.session import read_session
from quantnet_controller.db.sqla.model.bsmnode import add_bsmnode, get_bsmnode, list_bsmnodes
from quantnet_controller.db.sqla.model.qnode import add_qnode, get_qnode, list_qnodes
from quantnet_controller.db.sqla.model.mnode import add_mnode, get_mnode, list_mnodes
from quantnet_controller.db.sqla.model.switch import add_switch, get_switch, list_switches


class Node:
    def add(self, desc, **kwargs) -> str:
        return self.save_node(desc, **kwargs)

    def list(self, **kwargs):
        return self.list_nodes(**kwargs)

    def get(self, id, **kwargs):
        """ Get the records from the table. It allows filtering based on "id"

        parameters:
        -----------
        id: str or dict
            either id string or filter dict

        """

        return self.get_nodes(id, **kwargs)

    def update(self, id, key, value, **kwargs):
        raise Exception(f"{self.__class__.__name__}::update not implemented.")

    def delete(self, id, **kwargs):
        raise Exception(f"{self.__class__.__name__}::delete not implemented.")

    def exist(self, id, **kwargs):
        raise Exception(f"{self.__class__.__name__}::exist not implemented.")

    @read_session
    def get_nodes(self, id: dict, *, session, **kwargs):
        """Returns a node for the given id.

        :param id: the filter, i.e. {"systemSettings.ID": "LBNL-Q"}.
        :param session: the database session in use.

        :returns: a dict with all information for the node.
        """

        if not isinstance(id, dict) and not isinstance(id, str):
            return {}

        if isinstance(id, str):
            functions = [get_qnode, get_mnode, get_bsmnode, get_switch]
            for func in functions:
                device = func(id)
                if device:
                    return device
            return {}

        try:
            new_id = {}
            for k, v in id.items():
                if k.split(".")[0] != "systemSettings":
                    return {}
                new_id.update({k.split(".")[1]: v})

            query = session.query(models.SystemSetting).filter_by(**new_id)

            setting = query.first()
        except exc.NoResultFound:
            raise exception.NodeNotFound(f"Qnode with ID '{id}' cannot be found")

        if not setting:
            raise exception.NodeNotFound(f"Qnode with ID '{id}' cannot be found")

        try:
            if setting.type == "QNode" or setting.type == "QRepeater":
                device = get_qnode(setting.qnode_id)
            elif setting.type == "MNode":
                device = get_mnode(setting.mnode_id)
            elif setting.type == "BSMNode":
                device = get_bsmnode(setting.bsmnode_id)
            elif setting.type == "OpticalSwitch":
                device = get_switch(setting.switch_id)
            else:
                raise exception.InvalidType(f"Unknown device type: {setting.type}")
        except Exception:
            raise exception.QuantnetException(f"Cannot find device: {id}")

        return device

    def list_nodes(self):
        return list_qnodes({}) + list_bsmnodes({}) + list_mnodes({}) + list_switches({})

    def save_node(self, desc, **kwargs):
        """ Save the data in desc to database

        :param desc: contain the data to be saved

        :returns: uuid, type, jsobj: the information on the saved data
        """
        if isinstance(desc, QNode):
            type = NodeType.QUANTUM
            jsobj = json.loads(desc.serialize())
        if isinstance(desc, QRepeater):
            type = NodeType.QRepeater
            jsobj = json.loads(desc.serialize())
        elif isinstance(desc, MNode):
            type = NodeType.M
            jsobj = json.loads(desc.serialize())
        elif isinstance(desc, BSMNode):
            type = NodeType.BSM
            jsobj = json.loads(desc.serialize())
        elif isinstance(desc, OpticalSwitch):
            type = NodeType.SWITCH
            jsobj = json.loads(desc.serialize())
        elif isinstance(desc, dict):
            jsobj = desc
            try:
                type = NodeType(jsobj["systemSettings"]["type"])
            except Exception:
                raise Exception(
                    f"Unknown value in the systemSetting.type: \
                         {jsobj['systemSettings']['type']}.")
        else:
            raise exception.InvalidType("Invalid type in arguments.")

        uuid = generate_uuid()
        try:
            if type == NodeType.QUANTUM or type == NodeType.QR:
                add_qnode(
                    id=uuid,
                    system_settings=jsobj['systemSettings'],
                    qubit_settings=jsobj['qubitSettings'],
                    interface_settings=jsobj['matterLightInterfaceSettings'],
                    channel_settings=jsobj['channels'])
            elif type == NodeType.SWITCH:
                add_switch(
                    id=uuid,
                    system_settings=jsobj['systemSettings'],
                    channels=jsobj['channels'])
            elif type == NodeType.M:
                add_mnode(
                    id=uuid,
                    system_settings=jsobj['systemSettings'],
                    quantum_settings=jsobj['quantumSettings'],
                    channels=jsobj['channels'])
            elif type == NodeType.BSM:
                add_bsmnode(
                    id=uuid,
                    system_settings=jsobj['systemSettings'],
                    quantum_settings=jsobj['quantumSettings'],
                    channels=jsobj['channels'])
            else:
                raise exception.InvalidType(f"Unknown Node type: {type}")
        except Exception as e:
            raise exception.QuantnetException(e)

        return uuid, type, jsobj

    def read_node(self, uuid):
        """ Return a node with given uuid.

        :param uuid: the uuid of the node

        :returns: a dict with information for the node
        """

        self._output = get_qnode(uuid)
        if not self._output.is_empty():
            self._type = NodeType.QUANTUM
            return self._output

        self._output = get_bsmnode(uuid)
        if not self._output.is_empty():
            self._type = NodeType.BSM
            return self._output

        self._output = get_mnode(uuid)
        if not self._output.is_empty():
            self._type = NodeType.M
            return self._output

        self._output = get_switch(uuid)
        if not self._output.is_empty():
            self._type = NodeType.SWITCH
            return self._output

        return {}
