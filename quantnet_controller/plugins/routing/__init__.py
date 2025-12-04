import logging
from quantnet_controller.common.plugin import RoutingPlugin, PluginType, Path
from quantnet_controller.plugins.routing.routing import NetworkGenerator, RALG

log = logging.getLogger("plugins.routing")


class PathFinder(RoutingPlugin):
    def __init__(self, context):
        super().__init__("router", PluginType.ROUTING, context=context)
        self._network = None
        self.ctx = context

    def initialize(self):
        pass

    def destroy(self):
        pass

    def reset(self):
        pass

    def start(self):
        log.info("PathFinder routing module started")

        # create the network including graph from the topology
        self._network = NetworkGenerator(resource_mgr=self.ctx.rm)

    async def find_shortest_path(self, src, dst) -> Path:
        """
        find one shortest path through a quantum network topology

        :param src: Source node
        :type src: str
        :param dst: Destination node
        :type dst: str
        :return: A Path object
        :rtype: list
        """
        route_cadidates = self._network.find_route(src, dst, ent_link=True)
        if not route_cadidates:
            raise Exception("no route found")
        shortest_route = route_cadidates[0]
        hops = len(shortest_route)
        for r in route_cadidates:
            if len(r) < hops:
                hops = len(r)
                shortest_route = r

        # Return the resources on the route
        resources = self._network.get_resources(shortest_route)

        # Fill and return the Path
        return Path(hops=resources)

    async def find_all_shortest_paths(self, src, dst) -> list:
        """
        find all shortest paths through a quantum network topology

        :param src: Source node
        :type src: str
        :param dst: Destination node
        :type dst: str
        :return: A list of Path objects
        :rtype: list
        """
        # Calculate the route between src and dst
        route_cadidates = self._network.find_route(src, dst, ent_link=True,
                                                   algorithm={"name": RALG.all_shortest.value})
        if not route_cadidates:
            raise Exception("no route found")

        shortest_routes = [route_cadidates[0]]
        hops = len(shortest_routes[0])
        for r in route_cadidates:
            if len(r) < hops:
                hops = len(r)
                shortest_routes = [r]
            elif len(r) == hops and r not in shortest_routes:
                shortest_routes.append(r)

        # Return the resources on the route
        paths = []
        for shortest_route in shortest_routes:
            resources = self._network.get_resources(shortest_route)
            paths.append(Path(hops=resources))

        return paths

    async def find_all_paths(self, src, dst) -> list:
        """
        find all paths through a quantum network topology

        :param src: Source node
        :type src: str
        :param dst: Destination node
        :type dst: str
        :return: A list of Path objects
        :rtype: list
        """
        # Calculate the route between src and dst
        route_cadidates = self._network.find_route(src, dst, ent_link=True,
                                                   algorithm={"name": RALG.all_simple_paths.value})
        if not route_cadidates:
            raise Exception("no route found")

        # Return the resources on the route
        paths = []
        for route in route_cadidates:
            resources = self._network.get_resources(route)
            paths.append(Path(hops=resources))

        return paths

    async def find_path(self, src, dst) -> Path:
        return await self.find_shortest_path(src, dst)
