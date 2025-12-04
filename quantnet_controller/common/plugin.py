from quantnet_controller.core.managers import ControllerContextManager
from abc import ABCMeta, abstractmethod
from enum import Enum, auto


class PluginType(Enum):
    ROUTING = auto()
    SCHEDULING = auto()
    MONITORING = auto()
    PROTOCOL = auto()


class Path:
    def __init__(self, hops=None):
        self._hops = hops

    @property
    def hops(self):
        return self._hops

    @hops.setter
    def hops(self, hops):
        self._hops = hops

    def __str__(self):
        return str(self.hops)

    def to_node_ids(self):
        """Extract node IDs from hops for serialization."""
        return (
            [str(hop.systemSettings.ID) for hop in self._hops if hasattr(hop, "systemSettings")] if self._hops else None
        )

    @classmethod
    def from_node_ids(cls, node_ids, resource_manager=None):
        """Create Path from list of node IDs.

        Args:
            node_ids: List of node ID strings
            resource_manager: Optional RM to fetch full node objects

        Returns:
            Path object with hops as node IDs or full objects
        """
        if node_ids is None:
            return cls(hops=None)

        if resource_manager:
            # Fetch full node objects from database
            nodes = resource_manager.get_nodes(*node_ids)
            return cls(hops=nodes)
        else:
            # Store as list of IDs (lightweight)
            return cls(hops=node_ids)


class Plugin(metaclass=ABCMeta):
    """
    Plugin abstract base class

    Example::

        p = Plugin("My Plugin", PluginType.ROUTING)

    :param str: Name of the plugin
    :param ptype: Plugin type
    :param context: Active context from the running Controller instance

    """

    def __init__(self, name: str, ptype: str, context: ControllerContextManager = None):
        self._name = name
        self._type = ptype
        self._context = context
        self._client_commands = list()
        self._server_commands = list()
        self._msg_commands = list()

    def __str__(self):
        return f"{self._name} {self._type}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def pluginType(self) -> str:
        return self._type

    @property
    def serverContext(self) -> object:
        return self._context

    @abstractmethod
    def initialize(self):
        """
        Abstract plugin initializtion method
        """
        pass

    @abstractmethod
    def destroy(self):
        """
        Abstract plugin destroy method
        """
        pass

    @abstractmethod
    def reset(self):
        """
        Abstract plugin reset method
        """
        pass

    def get_client_commands(self) -> list:
        """
        :returns: A list of registered RPC client protocol commands
        :rtype: list
        """
        return self._client_commands

    def get_server_commands(self) -> list:
        """
        :returns: A list of registered RPC server protocol commands
        :rtype: list
        """
        return self._server_commands

    def get_msg_commands(self) -> list:
        """
        :returns: A list of registered MsgServer protocol commands (topics)
        :rtype: list
        """
        return self._msg_commands


class RoutingPlugin(Plugin):
    @abstractmethod
    def find_path(self, src, dst) -> list:
        """
        find a path through a quantum network topology

        Example::

            path = find_path("nodeA", "nodeB")

        :param src: Source node
        :param dst: Destination node
        :returns: A list of Node objects
        :rtype: list
        """
        pass


class SchedulingPlugin(Plugin):
    @abstractmethod
    def start(self):
        """
        Abstract method to start the scheduling plugin
        """
        pass

    @abstractmethod
    def schedule(self):
        """
        Abstract method to invoke scheduling backend
        """
        pass


class MonitoringPlugin(Plugin):
    @abstractmethod
    def handle_resource_update(self):
        """
        Abstract method to handle resource updates on the monitoring MsgServer topic
        """
        pass


class ProtocolPlugin(Plugin):
    def __init__(self, name: str, ptype: str, context: ControllerContextManager = None):
        super().__init__(name, ptype, context)
        self._client_commands = list()
        self._server_commands = list()
        self._msg_commands = list()
        self._exp_path = list()

    @property
    def exp_path(self) -> object:
        return self._exp_path

    @exp_path.setter
    def exp_path(self, path: object):
        self._exp_path = path

    def get_schema_paths(self) -> list:
        """
        :returns: A list of registered schema metadata
        :rtype: list
        """
        return self._schema
