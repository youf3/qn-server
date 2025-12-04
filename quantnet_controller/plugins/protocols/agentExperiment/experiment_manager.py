# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)


def match_agent_to_exp(exp_def, path):
    """Match agents to experiment definition based on path."""
    mapping = []
    nodes = [x for x in path.hops if x.systemSettings.type != "OpticalSwitch"]
    exp_nodes = [x.node_type for x in exp_def.agent_sequences]
    for node_type in exp_nodes:
        for i in range(len(nodes)):
            if node_type == nodes[i].systemSettings.type:
                mapping.append(nodes[i].systemSettings.ID)
                nodes.remove(nodes[i])
                break

    logger.info(f"Found agents {mapping}")
    return mapping
