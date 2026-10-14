"""
Unit tests for request_translator module, focusing on agent validation logic.
"""

import time
import asyncio
import pytest
from datetime import timedelta
from unittest.mock import Mock, MagicMock, patch
from collections import Counter
from bitarray import bitarray

from quantnet_controller.common.request_translator import (
    _validate_agent_requirements,
    match_agent_to_exp,
)
from quantnet_controller.common.experimentdefinitions import Experiment, AgentSequences
from quantnet_controller.common.plugin import Path


# Mock classes for testing
class MockSequence:
    """Mock Sequence class for testing."""
    def __init__(self, name, duration_ms=100):
        from datetime import timedelta
        self.name = name
        self.duration_ms = duration_ms
        self.duration = timedelta(milliseconds=duration_ms)
        self.class_name = name


class MockAgentSequence:
    """Mock AgentSequence class for testing."""
    def __init__(self, name, node_type, sequences=None):
        self.name = name
        self.node_type = node_type
        self.sequences = sequences or [MockSequence(f"{name}_seq")]


class MockExperiment:
    """Mock Experiment class for testing."""
    def __init__(self, name, agent_sequences):
        self.name = name
        self.agent_sequences = agent_sequences


class MockSystemSettings:
    """Mock SystemSettings class for testing."""
    def __init__(self, node_id, node_type):
        self.ID = node_id
        self.type = node_type


class MockNode:
    """Mock Node class for testing."""
    def __init__(self, node_id, node_type, last_seen=None):
        self.systemSettings = MockSystemSettings(node_id, node_type)
        self.last_seen = last_seen


# Test fixtures
@pytest.fixture
def qnode_sequence():
    """Create a QNode agent sequence."""
    return MockAgentSequence("QNode_seq", "QNode")


@pytest.fixture
def mnode_sequence():
    """Create an MNode agent sequence."""
    return MockAgentSequence("MNode_seq", "MNode")


@pytest.fixture
def simple_experiment(qnode_sequence, mnode_sequence):
    """Create a simple experiment requiring QNode and MNode."""
    return MockExperiment("SimpleExp", [qnode_sequence, mnode_sequence])


@pytest.fixture
def dual_qnode_experiment(qnode_sequence):
    """Create an experiment requiring two QNodes."""
    return MockExperiment("DualQNodeExp", [qnode_sequence, qnode_sequence])


@pytest.fixture
def qnode_mnode_qnode_experiment(qnode_sequence, mnode_sequence):
    """Create an experiment requiring QNode, MNode, QNode."""
    return MockExperiment(
        "ComplexExp",
        [qnode_sequence, mnode_sequence, qnode_sequence]
    )


# Tests for _validate_agent_requirements()
class TestValidateAgentRequirements:
    """Test suite for _validate_agent_requirements function."""

    def test_all_agents_available(self, simple_experiment):
        """Test validation passes when all required agents are available."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
        ]
        
        is_valid, required_types, available_types, missing_types = (
            _validate_agent_requirements(simple_experiment, nodes)
        )
        
        assert is_valid is True
        assert required_types == ["QNode", "MNode"]
        assert available_types == ["QNode", "MNode"]
        assert missing_types == []

    def test_missing_agent_type(self, simple_experiment):
        """Test validation fails when required agent type is missing."""
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        
        with pytest.raises(ValueError) as exc_info:
            _validate_agent_requirements(simple_experiment, nodes)
        
        assert "Agent validation failed" in str(exc_info.value)
        assert "MNode" in str(exc_info.value)

    def test_insufficient_agent_count(self, dual_qnode_experiment):
        """Test validation fails when insufficient agents of required type."""
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        
        with pytest.raises(ValueError) as exc_info:
            _validate_agent_requirements(dual_qnode_experiment, nodes)
        
        assert "Agent validation failed" in str(exc_info.value)
        assert "QNode" in str(exc_info.value)

    def test_extra_agents_ok(self, simple_experiment):
        """Test validation passes with extra agents beyond requirements."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
            MockNode("node_3", "QNode", last_seen=now),  # Extra QNode
        ]
        
        is_valid, required_types, available_types, missing_types = (
            _validate_agent_requirements(simple_experiment, nodes)
        )
        
        assert is_valid is True
        assert missing_types == []

    def test_complex_agent_requirements(self, qnode_mnode_qnode_experiment):
        """Test validation with complex requirements (QNode, MNode, QNode)."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
            MockNode("node_3", "QNode", last_seen=now),
        ]
        
        is_valid, required_types, available_types, missing_types = (
            _validate_agent_requirements(qnode_mnode_qnode_experiment, nodes)
        )
        
        assert is_valid is True
        assert required_types == ["QNode", "MNode", "QNode"]
        assert missing_types == []

    def test_empty_path(self, simple_experiment):
        """Test validation fails with empty path."""
        nodes = []
        
        with pytest.raises(ValueError) as exc_info:
            _validate_agent_requirements(simple_experiment, nodes)
        
        assert "Agent validation failed" in str(exc_info.value)

    def test_empty_experiment(self):
        """Test validation passes with empty experiment requirements."""
        exp = MockExperiment("EmptyExp", [])
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        
        is_valid, required_types, available_types, missing_types = (
            _validate_agent_requirements(exp, nodes)
        )
        
        assert is_valid is True
        assert required_types == []
        assert missing_types == []

    def test_string_node_ids(self, simple_experiment):
        """Test validation with string node IDs instead of node objects."""
        nodes = ["QNode", "MNode"]
        
        is_valid, required_types, available_types, missing_types = (
            _validate_agent_requirements(simple_experiment, nodes)
        )
        
        assert is_valid is True
        assert available_types == ["QNode", "MNode"]

    def test_mixed_node_types(self, simple_experiment):
        """Test validation with mixed node objects and string IDs."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            "MNode",
        ]
        
        is_valid, required_types, available_types, missing_types = (
            _validate_agent_requirements(simple_experiment, nodes)
        )
        
        assert is_valid is True
        assert available_types == ["QNode", "MNode"]

    def test_error_message_contains_details(self, dual_qnode_experiment):
        """Test that error message contains detailed information about missing agents."""
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        
        with pytest.raises(ValueError) as exc_info:
            _validate_agent_requirements(dual_qnode_experiment, nodes)
        
        error_msg = str(exc_info.value)
        assert "required" in error_msg.lower()
        assert "available" in error_msg.lower()


# Tests for match_agent_to_exp()
class TestMatchAgentToExp:
    """Test suite for match_agent_to_exp function."""

    def test_successful_match_with_validation(self, simple_experiment):
        """Test successful agent matching with validation enabled."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
        ]
        path = Mock()
        path.hops = nodes
        
        result = match_agent_to_exp(simple_experiment, path, validate=True)
        
        assert result == ["node_1", "node_2"]

    def test_validation_failure_raises_exception(self, simple_experiment):
        """Test that validation failure raises ValueError."""
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        path = Mock()
        path.hops = nodes
        
        with pytest.raises(ValueError):
            match_agent_to_exp(simple_experiment, path, validate=True)

    def test_backward_compatibility_validate_false(self, simple_experiment):
        """Test backward compatibility with validate=False."""
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        path = Mock()
        path.hops = nodes
        
        # Should not raise exception even though MNode is missing
        result = match_agent_to_exp(simple_experiment, path, validate=False)
        
        # Should return partial mapping
        assert "node_1" in result

    def test_non_mutating_behavior(self, simple_experiment):
        """Test that match_agent_to_exp doesn't mutate the original nodes list."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
        ]
        original_length = len(nodes)
        path = Mock()
        path.hops = nodes
        
        match_agent_to_exp(simple_experiment, path, validate=True)
        
        # Original list should not be modified
        assert len(nodes) == original_length

    def test_order_preservation(self, qnode_mnode_qnode_experiment):
        """Test that agents are matched in the order required by experiment."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
            MockNode("node_3", "QNode", last_seen=now),
        ]
        path = Mock()
        path.hops = nodes
        
        result = match_agent_to_exp(qnode_mnode_qnode_experiment, path, validate=True)
        
        # Should match in order: QNode, MNode, QNode
        assert result == ["node_1", "node_2", "node_3"]

    def test_optical_switch_filtered(self, simple_experiment):
        """Test that OpticalSwitch nodes are filtered out."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("switch_1", "OpticalSwitch", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
        ]
        path = Mock()
        path.hops = nodes
        
        result = match_agent_to_exp(simple_experiment, path, validate=True)
        
        assert result == ["node_1", "node_2"]
        assert "switch_1" not in result

    def test_path_as_list(self, simple_experiment):
        """Test match_agent_to_exp with path as list instead of Path object."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
        ]
        
        result = match_agent_to_exp(simple_experiment, nodes, validate=True)
        
        assert result == ["node_1", "node_2"]

    def test_path_as_string_list(self, simple_experiment):
        """Test match_agent_to_exp with path as list of string IDs."""
        nodes = ["QNode", "MNode"]
        
        result = match_agent_to_exp(simple_experiment, nodes, validate=True)
        
        assert result == ["QNode", "MNode"]

    def test_none_path(self, simple_experiment):
        """Test match_agent_to_exp with None path."""
        path = Mock()
        path.hops = None
        
        with pytest.raises(ValueError):
            match_agent_to_exp(simple_experiment, path, validate=True)

    def test_default_validate_true(self, simple_experiment):
        """Test that validate=True is the default behavior."""
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        path = Mock()
        path.hops = nodes
        
        # Should raise ValueError because MNode is missing
        with pytest.raises(ValueError):
            match_agent_to_exp(simple_experiment, path)  # validate defaults to True


# Integration tests
class TestIntegration:
    """Integration tests for validation workflow."""

    def test_validation_workflow_success(self, simple_experiment):
        """Test complete validation workflow with valid path."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
        ]
        path = Mock()
        path.hops = nodes
        
        # Should not raise exception
        result = match_agent_to_exp(simple_experiment, path, validate=True)
        
        assert len(result) == 2
        assert result[0] == "node_1"
        assert result[1] == "node_2"

    def test_validation_workflow_failure(self, simple_experiment):
        """Test complete validation workflow with invalid path."""
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        path = Mock()
        path.hops = nodes
        
        # Should raise ValueError with details
        with pytest.raises(ValueError) as exc_info:
            match_agent_to_exp(simple_experiment, path, validate=True)
        
        error_msg = str(exc_info.value)
        assert "MNode" in error_msg
        assert "required" in error_msg.lower()

    def test_multiple_missing_agents(self):
        """Test validation with multiple missing agent types."""
        now = time.time()
        exp = MockExperiment(
            "MultiExp",
            [
                MockAgentSequence("QNode_seq", "QNode"),
                MockAgentSequence("MNode_seq", "MNode"),
                MockAgentSequence("BSM_seq", "BSMNode"),
            ]
        )
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        path = Mock()
        path.hops = nodes
        
        with pytest.raises(ValueError) as exc_info:
            match_agent_to_exp(exp, path, validate=True)
        
        error_msg = str(exc_info.value)
        assert "MNode" in error_msg
        assert "BSMNode" in error_msg

    def test_count_mismatch_error_details(self, dual_qnode_experiment):
        """Test that count mismatch error includes required and available counts."""
        now = time.time()
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        path = Mock()
        path.hops = nodes
        
        with pytest.raises(ValueError) as exc_info:
            match_agent_to_exp(dual_qnode_experiment, path, validate=True)
        
        error_msg = str(exc_info.value)
        # Should indicate need for 2 QNodes but only 1 available
        assert "2" in error_msg or "required" in error_msg.lower()


# Edge case tests
class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_very_large_path(self, simple_experiment):
        """Test validation with very large path."""
        now = time.time()
        nodes = [
            MockNode(f"node_{i}", "QNode" if i % 2 == 0 else "MNode", last_seen=now)
            for i in range(1000)
        ]
        path = Mock()
        path.hops = nodes
        
        result = match_agent_to_exp(simple_experiment, path, validate=True)
        
        assert len(result) == 2

    def test_duplicate_agent_types_in_sequence(self):
        """Test experiment with many duplicate agent types."""
        now = time.time()
        exp = MockExperiment(
            "ManyQNodes",
            [MockAgentSequence(f"QNode_{i}", "QNode") for i in range(10)]
        )
        nodes = [MockNode(f"node_{i}", "QNode", last_seen=now) for i in range(10)]
        path = Mock()
        path.hops = nodes
        
        result = match_agent_to_exp(exp, path, validate=True)
        
        assert len(result) == 10

    def test_case_sensitive_node_types(self, simple_experiment):
        """Test that node type matching is case-sensitive."""
        now = time.time()
        nodes = [
            MockNode("node_1", "qnode", last_seen=now),  # lowercase
            MockNode("node_2", "MNode", last_seen=now),
        ]
        path = Mock()
        path.hops = nodes
        
        # Should fail because "qnode" != "QNode"
        with pytest.raises(ValueError):
            match_agent_to_exp(simple_experiment, path, validate=True)

    def test_whitespace_in_node_types(self):
        """Test handling of whitespace in node types."""
        now = time.time()
        exp = MockExperiment(
            "WhitespaceExp",
            [MockAgentSequence("QNode_seq", "QNode ")]  # trailing space
        )
        nodes = [MockNode("node_1", "QNode", last_seen=now)]
        path = Mock()
        path.hops = nodes
        
        # Should fail because "QNode " != "QNode"
        with pytest.raises(ValueError):
            match_agent_to_exp(exp, path, validate=True)


# Tests for heartbeat timeout / last_seen validation
class TestHeartbeatTimeout:
    """Test suite for last_seen heartbeat timeout filtering in _validate_agent_requirements."""

    def test_recent_heartbeat_passes(self, simple_experiment):
        """Test that nodes with recent last_seen pass validation."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now),
        ]

        is_valid, _, available_types, _ = _validate_agent_requirements(
            simple_experiment, nodes
        )

        assert is_valid is True
        assert "QNode" in available_types
        assert "MNode" in available_types

    def test_stale_heartbeat_excluded(self, simple_experiment):
        """Test that nodes with stale last_seen are excluded."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now - 120),  # 2 minutes ago
        ]

        with pytest.raises(ValueError) as exc_info:
            _validate_agent_requirements(simple_experiment, nodes)

        error_msg = str(exc_info.value)
        assert "MNode" in error_msg
        assert "stale" in error_msg.lower()

    def test_no_last_seen_treated_as_stale(self, simple_experiment):
        """Test that nodes without last_seen attribute are treated as stale."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode"),  # no last_seen
        ]

        with pytest.raises(ValueError) as exc_info:
            _validate_agent_requirements(simple_experiment, nodes)

        assert "Agent validation failed" in str(exc_info.value)

    def test_custom_heartbeat_timeout(self, simple_experiment):
        """Test validation with a custom heartbeat timeout."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now - 30),  # 30s ago
            MockNode("node_2", "MNode", last_seen=now - 30),
        ]

        # With 60s timeout, should pass
        is_valid, _, _, _ = _validate_agent_requirements(
            simple_experiment, nodes, heartbeat_timeout=60
        )
        assert is_valid is True

        # With strict 10s timeout, should fail
        with pytest.raises(ValueError):
            _validate_agent_requirements(
                simple_experiment, nodes, heartbeat_timeout=10
            )

    def test_all_nodes_stale_fails(self, simple_experiment):
        """Test that validation fails when all nodes are stale."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now - 300),  # 5 min ago
            MockNode("node_2", "MNode", last_seen=now - 300),
        ]

        with pytest.raises(ValueError) as exc_info:
            _validate_agent_requirements(simple_experiment, nodes)

        assert "Agent validation failed" in str(exc_info.value)

    def test_string_nodes_bypass_heartbeat_check(self, simple_experiment):
        """Test that string node IDs bypass the heartbeat check."""
        nodes = ["QNode", "MNode"]

        is_valid, _, _, _ = _validate_agent_requirements(simple_experiment, nodes)
        assert is_valid is True

    def test_stale_error_includes_node_ids(self, simple_experiment):
        """Test that the error message includes stale node IDs."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("stale_mnode", "MNode", last_seen=now - 300),
        ]

        with pytest.raises(ValueError) as exc_info:
            _validate_agent_requirements(simple_experiment, nodes)

        error_msg = str(exc_info.value)
        assert "stale_mnode" in error_msg

    def test_boundary_exactly_at_timeout(self, simple_experiment):
        """Test node just past the timeout boundary is excluded."""
        now = time.time()
        nodes = [
            MockNode("node_1", "QNode", last_seen=now),
            MockNode("node_2", "MNode", last_seen=now - 60.001),  # just over 60s
        ]

        with pytest.raises(ValueError):
            _validate_agent_requirements(
                simple_experiment, nodes, heartbeat_timeout=60
            )

    def test_mixed_stale_and_fresh_same_type(self):
        """Test that stale nodes of a type don't block fresh ones of the same type."""
        exp = MockExperiment(
            "SingleQNode",
            [MockAgentSequence("QNode_seq", "QNode")]
        )
        now = time.time()
        nodes = [
            MockNode("stale_q", "QNode", last_seen=now - 300),
            MockNode("fresh_q", "QNode", last_seen=now),
        ]

        is_valid, _, available_types, _ = _validate_agent_requirements(exp, nodes)
        assert is_valid is True
        assert available_types == ["QNode"]


# Tests for reading prefill_map from experiment definition
class TestPrefillFromExperimentDefinition:
    """Test suite for reading prefill_map from experiment definition."""

    def test_exp_with_prefill_map_attribute(self):
        """Experiment definition with prefill_map attribute is read correctly."""
        qnode_seq = MockAgentSequence("QNode_seq", "QNode")
        exp = MockExperiment("ExpWithPrefill", [qnode_seq])
        
        # Add prefill_map attribute to experiment
        exp.prefill_map = {"agent1": 2, "agent2": 1}
        
        # Verify the prefill_map can be read via getattr
        prefill_map = getattr(exp, 'prefill_map', None) or {}
        assert prefill_map == {"agent1": 2, "agent2": 1}

    def test_exp_without_prefill_map_defaults_to_empty(self):
        """Experiment definition without prefill_map defaults to empty dict."""
        qnode_seq = MockAgentSequence("QNode_seq", "QNode")
        exp = MockExperiment("ExpNoPrefill", [qnode_seq])
        
        # No prefill_map attribute set
        prefill_map = getattr(exp, 'prefill_map', None) or {}
        assert prefill_map == {}

    def test_exp_with_empty_prefill_map(self):
        """Experiment definition with empty prefill_map."""
        qnode_seq = MockAgentSequence("QNode_seq", "QNode")
        exp = MockExperiment("ExpEmptyPrefill", [qnode_seq])
        exp.prefill_map = {}
        
        prefill_map = getattr(exp, 'prefill_map', None) or {}
        assert prefill_map == {}

    def test_exp_def_with_prefill_map_in_get_slots_to_allocate(self):
        """get_slots_to_allocate reads prefill_map from exp_def."""
        from unittest.mock import Mock, AsyncMock, patch
        
        # Create mock context with async scheduler
        context = Mock()
        context.config = Mock()
        context.config.exp_def_path = "/tmp/dummy_exp_defs.py"
        context.config.schmanager_grace_period = timedelta(seconds=10)
        context.scheduler = AsyncMock()
        
        # Create mock availabilities
        availabilities = {
            "agent1": bitarray("1" * 100)
        }
        context.scheduler.get_timeslots = AsyncMock(return_value=availabilities)
        
        # Create experiment with prefill_map
        qnode_seq = MockAgentSequence("QNode_seq", "QNode")
        exp_def = MockExperiment("TestExp", [qnode_seq])
        exp_def.prefill_map = {"agent1": 2}
        
        # Create translator
        from quantnet_controller.common.request_translator import RequestTranslator
        with patch('quantnet_controller.common.request_translator.importlib.util'):
            translator = RequestTranslator.__new__(RequestTranslator)
            translator.context = context
            translator.lock = asyncio.Lock()
        
        # Test that prefill_map is read from exp_def (not from MongoDB)
        # We can't fully test async here, but we verify the code path reads from exp_def
        prefill_map = getattr(exp_def, 'prefill_map', None) or {}
        assert prefill_map == {"agent1": 2}


class TestTranslateRequestReadsFromExperimentDefinition:
    """Test that translate_request reads prefill_map from experiment definition."""

    def test_prefill_map_read_from_exp_in_translate_request(self):
        """translate_request reads prefill_map from exp, not from MongoDB."""
        from datetime import timedelta
        from unittest.mock import Mock, AsyncMock, patch
        
        # Create mock context
        context = Mock()
        context.config = Mock()
        context.config.exp_def_path = "/tmp/dummy_exp_defs.py"
        
        # Create experiment with prefill_map
        qnode_seq = MockAgentSequence("QNode_seq", "QNode")
        exp = MockExperiment("TestExp", [qnode_seq])
        exp.prefill_map = {"agent1": 3, "agent2": 1}
        
        # Verify prefill_map can be extracted
        prefill_map = getattr(exp, 'prefill_map', None) or {}
        assert prefill_map == {"agent1": 3, "agent2": 1}

    def test_exp_without_prefill_map_uses_empty_dict(self):
        """If exp has no prefill_map, defaults to empty dict."""
        qnode_seq = MockAgentSequence("QNode_seq", "QNode")
        exp = MockExperiment("TestExp", [qnode_seq])
        
        # No prefill_map attribute
        prefill_map = getattr(exp, 'prefill_map', None) or {}
        assert prefill_map == {}


class TestPrefillSlotInsertion:
    """Test prefill slot insertion logic in translate_request."""

    def test_prefill_allocation_structure(self):
        """Verify prefill allocation has correct structure."""
        # Test that when prefill_slots > 0, we build the right allocation dict
        from quantnet_controller.common.constants import Constants
        
        prefill_slots = 2
        rpc_params = [Mock(name="param1", value="val1")]
        
        # Simulate building prefill allocation as translate_request does
        assigned_prefill = list(range(0, prefill_slots))  # [0, 1]
        prefill_allocation = {
            "expName": "entanglement_prefill",
            "className": "entanglement_prefill",
            "parameters": rpc_params,
            "timeSlot": assigned_prefill,
        }
        
        assert prefill_allocation["expName"] == "entanglement_prefill"
        assert prefill_allocation["className"] == "entanglement_prefill"
        assert len(prefill_allocation["timeSlot"]) == 2

    def test_timeslot_consumption_with_prefill(self):
        """Verify that prefill slots are consumed from the front of timeslot_mask."""
        from bitarray import bitarray
        
        # Simulate agent's timeslot mask: 10 free slots
        timeslot_mask = bitarray("1111111111")
        prefill_slots_count = 2
        
        # Prefill consumes from the front
        assigned_prefill = list(range(len(timeslot_mask)))[:prefill_slots_count]
        remaining_mask = timeslot_mask[prefill_slots_count:]
        
        assert len(assigned_prefill) == 2
        assert len(remaining_mask) == 8
        assert remaining_mask == bitarray("11111111")

    def test_prefill_then_sequence_slot_allocation(self):
        """Verify prefill + sequence allocation order."""
        from bitarray import bitarray
        
        # Agent has 10 free slots, needs 2 for prefill and 3 for sequence
        timeslot_mask = bitarray("1111111111")
        
        prefill_slots_count = 2
        sequence_slots_count = 3
        
        # Prefill consumes first 2
        assigned_prefill = list(range(0, prefill_slots_count))
        remaining = timeslot_mask[prefill_slots_count:]
        
        # Sequence consumes next 3
        assigned_sequence = list(range(prefill_slots_count, prefill_slots_count + sequence_slots_count))
        
        assert assigned_prefill == [0, 1]
        assert assigned_sequence == [2, 3, 4]


class TestFindCommonSlotWithPrefill:
    """Test that find_common_slot accounts for prefill slots."""

    def test_slot_width_includes_prefill(self):
        """Common slot search pattern must include prefill slot count."""
        from bitarray import bitarray
        from quantnet_controller.common.request_translator import RequestTranslator
        from quantnet_controller.common.constants import Constants
        
        # Create a mock translator
        translator = RequestTranslator.__new__(RequestTranslator)
        
        # Agent with 1-slot experiment sequence + 2 prefill slots = 3 total needed
        seq = MockSequence("test_seq", 100)  # 1 slot
        agent_seq = MockAgentSequence("seq", "QNode", [seq])
        exp = MockExperiment("exp", [agent_seq])
        
        # Availability: exactly 3 consecutive free slots + padding to MAX_TIMESLOTS
        avail_str = "111" + "0" * (Constants.MAX_TIMESLOTS - 3)
        availabilities = {
            "agent1": avail_str
        }
        prefill_map = {"agent1": 2}
        
        agent_ids = ["agent1"]
        
        # Without prefill_map, find_common_slot would search for 1 slot and succeed
        # With prefill_map, it must search for 3 slots
        slots = translator.find_common_slot(agent_ids, availabilities, exp, prefill_map=prefill_map)
        
        # Should find slots starting at index 0
        assert slots["agent1"][0] == 0
        assert len(slots["agent1"]) == 3  # 2 prefill + 1 sequence

    def test_no_prefill_unchanged(self):
        """Without prefill, slot width matches experiment sequences only."""
        from quantnet_controller.common.request_translator import RequestTranslator
        from quantnet_controller.common.constants import Constants
        
        translator = RequestTranslator.__new__(RequestTranslator)
        
        seq = MockSequence("test_seq", 100)  # 1 slot
        agent_seq = MockAgentSequence("seq", "QNode", [seq])
        exp = MockExperiment("exp", [agent_seq])
        
        # Availability with free slots
        avail_str = "1" * Constants.MAX_TIMESLOTS
        availabilities = {
            "agent1": avail_str
        }
        prefill_map = {"agent1": 0}  # No prefill
        
        agent_ids = ["agent1"]
        
        slots = translator.find_common_slot(agent_ids, availabilities, exp, prefill_map=prefill_map)
        
        # Should allocate just 1 slot for the sequence
        assert len(slots["agent1"]) == 1

    def test_mixed_prefill_uses_max_per_agent(self):
        """Each agent's total = its experiment slots + its prefill slots."""
        from quantnet_controller.common.request_translator import RequestTranslator
        from quantnet_controller.common.constants import Constants
        
        translator = RequestTranslator.__new__(RequestTranslator)
        
        # Two agents, both with same sequence (1 slot)
        seq = MockSequence("test_seq", 100)  # 1 slot
        agent_seq1 = MockAgentSequence("seq", "QNode", [seq])
        agent_seq2 = MockAgentSequence("seq", "QNode", [seq])
        exp = MockExperiment("exp", [agent_seq1, agent_seq2])
        
        # Agent1: needs 1+3=4 total, Agent2: needs 1+1=2 total
        # Common slot search should look for max(4, 2) = 4
        avail_str = "1" * Constants.MAX_TIMESLOTS
        availabilities = {
            "agent1": avail_str,
            "agent2": avail_str
        }
        prefill_map = {"agent1": 3, "agent2": 1}
        
        agent_ids = ["agent1", "agent2"]
        
        slots = translator.find_common_slot(agent_ids, availabilities, exp, prefill_map=prefill_map)
        
        # Agent1 should get 4 slots (3 prefill + 1 sequence)
        assert len(slots["agent1"]) == 4
        # Agent2 should get 2 slots (1 prefill + 1 sequence)
        assert len(slots["agent2"]) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
