"""Tests for the monitoring plugin's link_state_update handling."""

import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from quantnet_controller.plugins.monitoring import Monitor
from quantnet_mq.schema.models import monitor


def make_context():
    """Create a minimal ControllerContextManager mock."""
    ctx = MagicMock()
    ctx.rm = MagicMock()
    ctx.rm.set_topo_updated = MagicMock()
    ctx.router = MagicMock()
    return ctx


def make_link_state_event(src_cid="QPU-1", dst_cid="BSM-1_2",
                          channel_id="1", state="QUANTUM_UP"):
    """Create a MonitorEvent with linkStateUpdate payload."""
    return monitor.MonitorEvent(
        rid=src_cid,
        ts=1727827200.0,
        eventType="linkStateUpdate",
        value={
            "src_cid": src_cid,
            "dst_cid": dst_cid,
            "channel_id": channel_id,
            "state": state,
        },
    )


class TestHandleLinkStateUpdate:
    def test_link_state_update_calls_set_topo_updated(self):
        """After processing a link_state_update event, the monitoring plugin
        must set the topology dirty flag via rm.set_topo_updated()."""
        ctx = make_context()
        mon = Monitor(ctx)
        value = {"src_cid": "QPU-1", "dst_cid": "BSM-1_2",
                 "channel_id": "1", "state": "QUANTUM_UP"}

        mock_link_db = MagicMock()
        mock_link_db.upsert = AsyncMock(return_value=True)
        with patch(
            "quantnet_controller.plugins.monitoring.AsyncDB"
        ) as mock_db_cls:
            mock_db_cls.return_value.handler.return_value = mock_link_db
            asyncio.run(mon._update_link_state(value))

        ctx.rm.set_topo_updated.assert_called_once()

    def test_periodic_link_telemetry_skips_topo_and_router_refresh(self):
        """When state_changed is False (periodic hello/probe telemetry),
        _update_link_state upserts the DB record but does NOT invalidate
        the topology cache or rebuild the routing graph."""
        ctx = make_context()
        mon = Monitor(ctx)
        value = {
            "src_cid": "QPU-1",
            "dst_cid": "QPU-2",
            "channel_id": "1",
            "state": "CONTROL_UP",
            "state_changed": False,
            "probe_tx_count": 5,
            "probe_ok_count": 5,
        }

        mock_link_db = MagicMock()
        mock_link_db.upsert = AsyncMock(return_value=True)
        with patch(
            "quantnet_controller.plugins.monitoring.AsyncDB"
        ) as mock_db_cls:
            mock_db_cls.return_value.handler.return_value = mock_link_db
            asyncio.run(mon._update_link_state(value))

        mock_link_db.upsert.assert_awaited_once()
        ctx.rm.set_topo_updated.assert_not_called()
        ctx.router.refresh.assert_not_called()

    def test_link_state_update_upserts_to_db_per_channel(self):
        """The monitoring plugin must upsert the link state keyed by
        (src_cid, dst_cid, channel_id)."""
        ctx = make_context()
        mon = Monitor(ctx)
        value = {"src_cid": "QPU-1", "dst_cid": "BSM-1_2",
                 "channel_id": "3", "state": "CONTROL_UP"}

        mock_link_db = MagicMock()
        mock_link_db.upsert = AsyncMock(return_value=True)
        with patch(
            "quantnet_controller.plugins.monitoring.AsyncDB"
        ) as mock_db_cls:
            mock_db_cls.return_value.handler.return_value = mock_link_db
            asyncio.run(mon._update_link_state(value))

        mock_link_db.upsert.assert_awaited_once()
        call_args = mock_link_db.upsert.call_args
        filter_arg = call_args[0][0]
        data_arg = call_args[0][1]
        assert filter_arg == {"src_cid": "QPU-1", "dst_cid": "BSM-1_2",
                              "channel_id": "3"}
        assert data_arg["state"] == "CONTROL_UP"
        assert data_arg["channel_id"] == "3"

    def test_link_state_event_dispatches_from_handle_resource_update(self):
        """A linkStateUpdate MonitorEvent arriving on the monitor topic
        must be dispatched to _handle_link_state_update."""
        ctx = make_context()
        mon = Monitor(ctx)

        event = make_link_state_event()
        serialized = event.serialize()

        with patch.object(mon, "_handle_link_state_update") as mock_handler:
            asyncio.run(mon.handle_resource_update(serialized))

        mock_handler.assert_called_once()


class TestResourceManagerLinkAdjacencyFilter:
    def test_link_is_up_classical_vs_quantum(self):
        """Classical channels require CONTROL_UP or QUANTUM_UP;
        quantum channels require QUANTUM_UP."""
        from quantnet_controller.core.managers import ResourceManager

        rm = ResourceManager.__new__(ResourceManager)
        with patch.object(rm, "_get_link_state", return_value="CONTROL_UP"):
            assert rm._link_is_up("LBNL-Q", "LBNL-SWITCH", "4", is_quantum=False) is True
            assert rm._link_is_up("LBNL-Q", "LBNL-SWITCH", "1", is_quantum=True) is False

        with patch.object(rm, "_get_link_state", return_value="QUANTUM_UP"):
            assert rm._link_is_up("LBNL-Q", "LBNL-SWITCH", "4", is_quantum=False) is True
            assert rm._link_is_up("LBNL-Q", "LBNL-SWITCH", "1", is_quantum=True) is True

        with patch.object(rm, "_get_link_state", return_value="INIT"):
            assert rm._link_is_up("LBNL-Q", "LBNL-SWITCH", "4", is_quantum=False) is False
            assert rm._link_is_up("LBNL-Q", "LBNL-SWITCH", "1", is_quantum=True) is False
