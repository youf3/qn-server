"""Tests for reactive routing graph refresh on link state change."""

import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from quantnet_controller.plugins.routing import PathFinder
from quantnet_controller.plugins.monitoring import Monitor


def make_context():
    ctx = MagicMock()
    ctx.rm = MagicMock()
    ctx.rm.set_topo_updated = MagicMock()
    ctx.router = MagicMock()
    ctx.router.refresh = MagicMock()
    return ctx


class TestPathFinderRefresh:
    def test_pathfinder_has_refresh_method(self):
        ctx = make_context()
        pf = PathFinder(ctx)
        assert hasattr(pf, "refresh")
        assert callable(pf.refresh)

    def test_refresh_calls_network_refresh_topology(self):
        ctx = make_context()
        pf = PathFinder(ctx)
        pf._network = MagicMock()
        pf.refresh()
        pf._network.refresh_topology.assert_called_once()

    def test_refresh_is_noop_before_start(self):
        """refresh() before start() should not crash (network is None)."""
        ctx = make_context()
        pf = PathFinder(ctx)
        # _network is None before start()
        pf.refresh()  # should not raise


class TestMonitorNotifiesRouter:
    def test_link_state_update_calls_router_refresh(self):
        """After a link state DB upsert, the monitor must call router.refresh()."""
        ctx = make_context()
        mon = Monitor(ctx)

        value = {
            "src_cid": "QPU-1",
            "dst_cid": "BSM-1_2",
            "channel_id": "1",
            "state": "QUANTUM_UP",
        }

        mock_link_db = MagicMock()
        mock_link_db.upsert = AsyncMock(return_value=True)
        with patch(
            "quantnet_controller.plugins.monitoring.AsyncDB"
        ) as mock_db_cls:
            mock_db_cls.return_value.handler.return_value = mock_link_db
            asyncio.run(mon._update_link_state(value))

        ctx.router.refresh.assert_called_once()

    def test_link_state_update_still_works_without_router(self):
        """If no router is configured, _update_link_state should not crash."""
        ctx = make_context()
        ctx.router = None
        mon = Monitor(ctx)

        value = {
            "src_cid": "QPU-1",
            "dst_cid": "BSM-1_2",
            "channel_id": "1",
            "state": "DOWN",
        }

        mock_link_db = MagicMock()
        mock_link_db.upsert = AsyncMock(return_value=True)
        with patch(
            "quantnet_controller.plugins.monitoring.AsyncDB"
        ) as mock_db_cls:
            mock_db_cls.return_value.handler.return_value = mock_link_db
            asyncio.run(mon._update_link_state(value))

        # Should not raise — rm.set_topo_updated still called
        ctx.rm.set_topo_updated.assert_called_once()
