#!/usr/bin/env python3
import logging
import unittest
from quantnet_controller.plugins.routing.routing import NetworkRouting, NetworkGenerator, draw_and_save_graph, RALG
from quantnet_controller.core.managers import ResourceManager
import networkx as nx

logger = logging.getLogger(__name__)
log_format = \
    '%(asctime)s - %(name)s - {%(filename)s:%(lineno)d} - [%(threadName)s] - %(levelname)s - %(message)s'
logging.basicConfig(handlers=[logging.StreamHandler()], format=log_format, force=True)


class TestRouting(unittest.IsolatedAsyncioTestCase):

    async def test_get_route_basic(self):
        network = nx.path_graph(5)
        route = NetworkRouting(network).get_routes(0, 4)
        print(route)

    async def test_get_route_algo_default(self):
        algo = NetworkRouting().routing_algorithm
        print(algo)

    async def test_set_route_algo(self):
        algo = NetworkRouting(algorithm=nx.bellman_ford_path).routing_algorithm
        print(algo)

    async def test_network_routing(self):
        class Config:
            def __init__(self):
                self.quantnet_api_base_url = None

        # Set the configuration
        config = Config()
        config.quantnet_api_base_url = "http://127.0.0.1:9000"

        # Get the network topology and represent it as a graph
        network = NetworkGenerator(config=config, resource_mgr=ResourceManager())

        # show the topology graph
        topology_graph = network.graph
        draw_and_save_graph(topology_graph)

        # show the entanglement graph
        ent_graph = network.ent_graph
        draw_and_save_graph(ent_graph)

        qnodes = network.qnodes

        # Find the route between nodes with shortest path algorithm
        routes = network.find_route(qnodes[-2], qnodes[-1])
        for index, route in enumerate(routes):
            print(f"Shortest Path #{index}: {route}")

        # Find shortest routes between nodes over entanglement link
        routes = network.find_route(qnodes[-2], qnodes[-1], ent_link=True,
                                    algorithm={"name": RALG.shortest.value})
        for index, route in enumerate(routes):
            print(f"Entanglement Path #{index}: {route}")
        if not route:
            return

        # Find shortest routes between nodes over entanglement link
        routes = network.find_route(qnodes[-2], qnodes[-1], ent_link=True,
                                    algorithm={"name": RALG.all_shortest.value})
        for index, route in enumerate(routes):
            print(f"Entanglement Path #{index}: {route}")
        if not route:
            return

        # Find all routes between nodes over entanglement link
        routes = network.find_route(qnodes[-2], qnodes[-1], ent_link=True,
                                    algorithm={"name": RALG.all_simple_paths.value})
        for index, route in enumerate(routes):
            print(f"Entanglement Path #{index}: {route}")
        if not route:
            return


        # Retrieve the resources along the path
        resources = network.get_resources(routes[0])
        print(f"Resources for the route {routes[0]}: {resources}")

        # Fill the path
        from quantnet_controller.common.plugin import Path
        Path(hops=resources)

