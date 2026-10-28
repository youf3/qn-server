#!/usr/bin/env python
"""
Link Control Lifecycle & End-to-End Routing Impact Regression Test.

Starts the controller (with QUANTNET_LINK_ADJACENCY=true) and 5 agents
(LBNL-Q, UCB-Q, LBNL-SWITCH, UCB-SWITCH, LBNL-BSM) as local subprocesses and
runs six verification phases:

Phase 1 (Initial Convergence & Baseline EGP):
  Verifies all 6 active quantum links reach QUANTUM_UP and all 9 classical
  links reach CONTROL_UP, then confirms a baseline EGP request succeeds.

Phase 2 (Interactive Multi-Probe Telemetry x3):
  Executes 3 consecutive link.control(action="probe", target="LBNL-SWITCH:1")
  calls on LBNL-Q (matching REPL `probe LBNL-SWITCH:1 3`) and verifies that all
  3 probe results return status=="ok" with rtt_ms, and that TX/OK counters on
  LBNL-Q ("LBNL-SWITCH:1") and RX/OK counters on LBNL-SWITCH ("LBNL-Q:1")
  increment by at least 3 with matching probe_history records.

Phase 3 (Administrative Disconnect -> Route Rejection):
  Executes link.control(action="disconnect", target="LBNL-SWITCH:1") on LBNL-Q,
  verifies the link transitions to DOWN on the monitor topic, and confirms
  that an EGP request between LBNL-Q and UCB-Q is rejected because the
  quantum edge is down.

Phase 4 (Administrative Connect + Probe -> Route Recovery):
  Executes link.control(action="connect", target="LBNL-SWITCH:1") followed by
  link.control(action="probe", target="LBNL-SWITCH:1") on LBNL-Q, verifies
  recovery to QUANTUM_UP, and confirms that an EGP request succeeds again.

Phase 5 (Administrative Parameter Tuning action="set"):
  Executes link.control(action="set", param="hold-time", value=45.0) and
  link.control(action="set", param="hello-interval", value=8.0) on LBNL-Q and
  verifies the updated hold_time via link.control(action="show").

Phase 6 (Log Cleanliness):
  Verifies no unexpected Tracebacks, Errno, FileNotFoundError, or
  InvalidStateError appear in controller or agent logs.

Usage:
  python -u regression_tests/scripts/link_control_lifecycle.py
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from quantnet_mq import Code
from quantnet_mq.msgserver import MsgServer
from quantnet_mq.rpcclient import RPCClient
from quantnet_mq.schema.models import Schema

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

WORKSPACE = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
)

AGENTS = {
    "LBNL-Q": "conf_lbnl-q.json",
    "UCB-Q": "conf_ucb-q.json",
    "LBNL-SWITCH": "conf_lbnl-switch.json",
    "UCB-SWITCH": "conf_ucb-switch.json",
    "LBNL-BSM": "conf_lbnl-bsm.json",
}

STRICT_EXPECTED_LINKS = {
    # Quantum outbound links (6)
    ("LBNL-Q", "LBNL-SWITCH", "1"): "QUANTUM_UP",
    ("UCB-Q", "UCB-SWITCH", "1"): "QUANTUM_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "14"): "QUANTUM_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "15"): "QUANTUM_UP",
    ("LBNL-SWITCH", "UCB-SWITCH", "99"): "QUANTUM_UP",
    ("UCB-SWITCH", "LBNL-SWITCH", "100"): "QUANTUM_UP",
    # Classical outbound links (9)
    ("LBNL-Q", "LBNL-SWITCH", "4"): "CONTROL_UP",
    ("UCB-Q", "UCB-SWITCH", "4"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-Q", "2"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-Q", "3"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "11"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "13"): "CONTROL_UP",
    ("UCB-SWITCH", "UCB-Q", "2"): "CONTROL_UP",
    ("UCB-SWITCH", "UCB-Q", "3"): "CONTROL_UP",
    ("LBNL-BSM", "LBNL-SWITCH", "2"): "CONTROL_UP",
}


def make_local_agent_cfg(workspace, template_cfg_path):
    """Read agent.cfg template and rewrite Docker paths to workspace-absolute paths."""
    with open(template_cfg_path, "r") as f:
        lines = f.readlines()

    workspace_cfg_path = os.path.join(workspace, "qn-agent", "config") + "/"
    new_lines = []
    for line in lines:
        if "path=" in line and "qn-agent/config/" in line:
            if "../qn-agent/config/" in line:
                line = line.replace("../qn-agent/config/", workspace_cfg_path)
            elif "/qn-agent/config/" in line:
                line = line.replace("/qn-agent/config/", workspace_cfg_path)
        new_lines.append(line)

    temp_file = tempfile.NamedTemporaryFile(
        mode="w", prefix="agent_cfg_", suffix=".cfg", delete=False
    )
    temp_file.writelines(new_lines)
    temp_file.close()
    return temp_file.name


def start_controller(log_files):
    """Start the quantnet controller with QUANTNET_LINK_ADJACENCY=true."""
    log_file = tempfile.NamedTemporaryFile(
        prefix="ctrl_lifecycle_", suffix=".log", delete=False
    )
    log_file.close()
    log_files["controller"] = log_file.name

    cmd = [
        sys.executable,
        "-m",
        "quantnet_controller.cli",
        "--mq-broker-host",
        "localhost",
        "--mq-mongo-host",
        "localhost",
        "-c",
        os.path.join(WORKSPACE, "etc", "quantnet.cfg"),
        "--plugin-path",
        os.path.join(WORKSPACE, "qn-plugins", "plugins"),
        "--schema-path",
        os.path.join(WORKSPACE, "qn-plugins", "plugins", "schema"),
    ]
    env = os.environ.copy()
    env["QUANTNET_LINK_ADJACENCY"] = "true"

    with open(log_file.name, "w") as f:
        proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env=env)
    return proc


def start_agent(agent_id, node_config, log_files):
    """Start a quantnet agent as a subprocess."""
    log_file = tempfile.NamedTemporaryFile(
        prefix=f"agent_{agent_id}_", suffix=".log", delete=False
    )
    log_file.close()
    log_files[agent_id] = log_file.name

    local_agent_cfg = make_local_agent_cfg(
        WORKSPACE, os.path.join(WORKSPACE, "qn-agent", "config", "agent.cfg")
    )

    cmd = [
        sys.executable,
        "-m",
        "quantnet_agent.cli",
        "-a",
        agent_id,
        "-n",
        node_config,
        "-c",
        local_agent_cfg,
        "--mq-broker-host",
        "localhost",
        "--no-repl",
        "--interpreter-path",
        os.path.join(WORKSPACE, "qn-plugins", "plugins", "pingpong", "interpreter"),
        "--schema-path",
        os.path.join(WORKSPACE, "qn-plugins", "plugins", "schema"),
    ]
    with open(log_file.name, "w") as f:
        proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
    return proc


def terminate_all(procs):
    """Terminate all subprocesses cleanly."""
    for proc in procs:
        try:
            proc.terminate()
        except Exception:
            pass
    for proc in procs:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def check_logs(log_files, exclude_patterns=None):
    """Scan log files for unexpected error patterns."""
    if exclude_patterns is None:
        exclude_patterns = []

    error_patterns = ["Traceback", "FileNotFoundError", "Errno", "InvalidStateError"]
    false_positives = [
        "RPC timeout",
        "--no-repl",
        "Cannot load local task",
    ] + exclude_patterns

    errors = []
    for name, path in log_files.items():
        try:
            with open(path, "r") as f:
                for line in f:
                    line = line.rstrip()
                    if any(pat in line for pat in error_patterns):
                        if not any(fp in line for fp in false_positives):
                            errors.append((name, line))
        except Exception as e:
            log.warning(f"Could not read log {path}: {e}")
    return errors


class StrictLinkMonitor:
    """Subscribes to MQTT monitor topic and tracks exact link states."""

    def __init__(self, expected_links, known_cids):
        self.expected = dict(expected_links)
        self.known_cids = set(known_cids)
        self.observed = {}
        self.history = {}

    async def on_message(self, raw):
        try:
            data = json.loads(raw)
            if data.get("eventType") != "linkStateUpdate":
                return
            value = data["value"]
            src = str(value["src_cid"])
            if src not in self.known_cids:
                return
            dst = str(value["dst_cid"])
            ch = str(value["channel_id"])
            state = str(value["state"])
            key = (src, dst, ch)
            self.observed[key] = state
            self.history.setdefault(key, []).append(state)
        except Exception:
            return

    async def wait_converged(self, timeout=60.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all(self.observed.get(k) == v for k, v in self.expected.items()):
                return True
            await asyncio.sleep(0.5)
        return all(self.observed.get(k) == v for k, v in self.expected.items())

    async def wait_for_link_state(self, key, target_state, timeout=20.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.observed.get(key) == target_state:
                return True
            await asyncio.sleep(0.25)
        return self.observed.get(key) == target_state


async def call_link_control(rpc_client, agent_id, **kwargs):
    """Send a link.control RPC to link/<agent_id> and return the parsed dict."""
    raw = await rpc_client.call(
        "link.control",
        kwargs,
        topic=f"link/{agent_id}",
        timeout=15.0,
    )
    return json.loads(raw)


async def call_egp(host="localhost"):
    """Send an EGP request (LBNL-Q -> UCB-Q) and return the parsed response dict."""
    Schema.load_schema(
        os.path.join(WORKSPACE, "qn-plugins", "plugins", "schema", "egp.yaml"),
        ns="egp",
    )
    client = RPCClient(f"egp-lifecycle-{int(time.time() * 1000)}", host=host)
    client.set_handler("egpRequest", None, "quantnet_mq.schema.models.egp.egpRequest")
    await client.start()
    try:
        msg = {
            "source": "LBNL-Q",
            "destination": "UCB-Q",
            "pairs": 1,
            "bellState": "∣Φ+⟩",
            "fidelity": 0.9,
        }
        raw = await client.call("egpRequest", msg, timeout=30.0)
        return json.loads(raw)
    finally:
        await client.stop()


async def main():
    procs = []
    log_files = {}
    msg_server = None
    rpc_client = None

    try:
        # 1. Start MQTT monitor subscriber first
        monitor = StrictLinkMonitor(STRICT_EXPECTED_LINKS, AGENTS.keys())
        msg_server = MsgServer(host="localhost", port=1883)
        msg_server.subscribe("monitor", monitor.on_message)
        await msg_server.start()

        # 2. Start controller (with QUANTNET_LINK_ADJACENCY=true)
        log.info("Starting controller (QUANTNET_LINK_ADJACENCY=true)...")
        procs.append(start_controller(log_files))
        await asyncio.sleep(4)

        # 3. Start all 5 agents
        log.info("Starting 5 agents...")
        topo_dir = os.path.join(
            WORKSPACE, "qn-mq", "quantnet_mq", "schema", "examples", "topology"
        )
        for agent_id, cfg_file in AGENTS.items():
            node_cfg = os.path.join(topo_dir, cfg_file)
            procs.append(start_agent(agent_id, node_cfg, log_files))
            await asyncio.sleep(0.5)

        # ── Phase 1: Initial Convergence & Baseline EGP ─────────────────────
        log.info("\n" + "=" * 70)
        log.info("PHASE 1: Waiting for strict link convergence and baseline EGP...")
        converged = await monitor.wait_converged(timeout=60.0)
        await asyncio.sleep(2.0)

        egp_base_resp = await call_egp()
        egp_base_code = egp_base_resp.get("status", {}).get("code")
        phase1_ok = converged and (egp_base_code == Code.OK.value)
        log.info(
            f"Phase 1: converged={converged}, baseline EGP code={egp_base_code}"
        )

        # Prepare RPC client for link.control
        rpc_client = RPCClient(f"lifecycle-ctrl-{os.getpid()}", host="localhost")
        rpc_client.set_handler(
            "link.control",
            None,
            "quantnet_mq.schema.models.link_adjacency.linkControl",
        )
        await rpc_client.start()

        # ── Phase 2: Multi-Probe Telemetry (x3) ─────────────────────────────
        log.info("\n" + "=" * 70)
        log.info("PHASE 2: Testing 3x on-demand link.control(action='probe')...")
        target_tx = "LBNL-SWITCH:1"
        target_rx = "LBNL-Q:1"

        pre_tx_resp = await call_link_control(
            rpc_client, "LBNL-Q", action="show_detail", target=target_tx
        )
        pre_rx_resp = await call_link_control(
            rpc_client, "LBNL-SWITCH", action="show_detail", target=target_rx
        )
        pre_tx_entry = pre_tx_resp.get("data") or {}
        pre_rx_entry = pre_rx_resp.get("data") or {}
        tx_before = pre_tx_entry.get("probe_tx_count", 0)
        tx_ok_before = pre_tx_entry.get("probe_ok_count", 0)
        rx_before = pre_rx_entry.get("probe_rx_count", 0)
        rx_ok_before = pre_rx_entry.get("probe_ok_count", 0)

        probe_results = []
        for _ in range(3):
            p_resp = await call_link_control(
                rpc_client, "LBNL-Q", action="probe", target=target_tx
            )
            probe_results.append(p_resp)
            await asyncio.sleep(0.15)

        log.info(f"3x probe responses: {probe_results}")
        probe3_results_ok = len(probe_results) == 3 and all(
            r.get("status") == "ok"
            and (r.get("data") or {}).get("status") == "ok"
            and (r.get("data") or {}).get("rtt_ms") is not None
            for r in probe_results
        )

        post_tx_resp = await call_link_control(
            rpc_client, "LBNL-Q", action="show_detail", target=target_tx
        )
        post_rx_resp = await call_link_control(
            rpc_client, "LBNL-SWITCH", action="show_detail", target=target_rx
        )
        post_tx_entry = post_tx_resp.get("data") or {}
        post_rx_entry = post_rx_resp.get("data") or {}

        tx_delta = post_tx_entry.get("probe_tx_count", 0) - tx_before
        tx_ok_delta = post_tx_entry.get("probe_ok_count", 0) - tx_ok_before
        rx_delta = post_rx_entry.get("probe_rx_count", 0) - rx_before
        rx_ok_delta = post_rx_entry.get("probe_ok_count", 0) - rx_ok_before
        tx_hist_len = len(post_tx_entry.get("probe_history") or [])
        rx_hist_len = len(post_rx_entry.get("probe_history") or [])

        phase2_ok = (
            probe3_results_ok
            and tx_delta >= 3
            and tx_ok_delta >= 3
            and rx_delta >= 3
            and rx_ok_delta >= 3
            and tx_hist_len >= 3
            and rx_hist_len >= 3
        )
        log.info(
            f"Phase 2: results_ok={probe3_results_ok}, tx_delta={tx_delta}, "
            f"rx_delta={rx_delta}, tx_hist={tx_hist_len}, rx_hist={rx_hist_len}"
        )

        # ── Phase 3: Administrative Disconnect -> EGP Rejected ──────────────
        log.info("\n" + "=" * 70)
        log.info("PHASE 3: Disconnecting LBNL-SWITCH:1 and verifying EGP rejection...")
        link_key = ("LBNL-Q", "LBNL-SWITCH", "1")
        disc_resp = await call_link_control(
            rpc_client, "LBNL-Q", action="disconnect", target=target_tx
        )
        disc_rpc_ok = disc_resp.get("status") == "ok"
        down_reached = await monitor.wait_for_link_state(link_key, "DOWN", timeout=15.0)
        await asyncio.sleep(1.0)

        egp_down_resp = await call_egp()
        egp_down_code = egp_down_resp.get("status", {}).get("code")
        egp_down_msg = egp_down_resp.get("status", {}).get("message", "")
        egp_rejected = egp_down_code in (Code.INVALID_ARGUMENT.value, Code.FAILED.value)
        phase3_ok = disc_rpc_ok and down_reached and egp_rejected
        log.info(
            f"Phase 3: disc_rpc_ok={disc_rpc_ok}, down_reached={down_reached}, "
            f"egp_down_code={egp_down_code} ({egp_down_msg})"
        )

        # ── Phase 4: Administrative Connect + Probe -> EGP Recovery ─────────
        log.info("\n" + "=" * 70)
        log.info("PHASE 4: Reconnecting + probing LBNL-SWITCH:1 and verifying EGP recovery...")
        conn_resp = await call_link_control(
            rpc_client, "LBNL-Q", action="connect", target=target_tx
        )
        conn_rpc_ok = conn_resp.get("status") == "ok"

        # Wait up to 12s for HELLO to bring link to CONTROL_UP or QUANTUM_UP
        deadline = time.monotonic() + 12.0
        while time.monotonic() < deadline:
            if monitor.observed.get(link_key) in ("CONTROL_UP", "QUANTUM_UP"):
                break
            await asyncio.sleep(0.3)

        reprobe_resp = await call_link_control(
            rpc_client, "LBNL-Q", action="probe", target=target_tx
        )
        reprobe_ok = reprobe_resp.get("status") == "ok"
        qup_reached = await monitor.wait_for_link_state(link_key, "QUANTUM_UP", timeout=15.0)
        await asyncio.sleep(1.5)

        egp_rec_resp = await call_egp()
        egp_rec_code = egp_rec_resp.get("status", {}).get("code")
        phase4_ok = (
            conn_rpc_ok
            and reprobe_ok
            and qup_reached
            and (egp_rec_code == Code.OK.value)
        )
        log.info(
            f"Phase 4: conn_rpc_ok={conn_rpc_ok}, reprobe_ok={reprobe_ok}, "
            f"qup_reached={qup_reached}, egp_rec_code={egp_rec_code}"
        )

        # ── Phase 5: Parameter Tuning via action='set' ──────────────────────
        log.info("\n" + "=" * 70)
        log.info("PHASE 5: Testing link.control(action='set')...")
        set_hold = await call_link_control(
            rpc_client, "LBNL-Q", action="set", param="hold-time", value=45
        )
        set_hello = await call_link_control(
            rpc_client, "LBNL-Q", action="set", param="hello-interval", value=8
        )
        show_resp = await call_link_control(rpc_client, "LBNL-Q", action="show")
        links_data = (show_resp.get("data") or {}).get("links") or {}
        hold_val = (links_data.get(target_tx) or {}).get("hold_time")
        phase5_ok = (
            set_hold.get("status") == "ok"
            and set_hello.get("status") == "ok"
            and hold_val == 45
        )
        log.info(f"Phase 5: set_hold={set_hold}, set_hello={set_hello}, hold_time={hold_val}")

        # ── Phase 6: Log Cleanliness ────────────────────────────────────────
        log.info("\n" + "=" * 70)
        log.info("PHASE 6: Checking controller and agent logs...")
        log_errors = check_logs(
            log_files,
            exclude_patterns=["Could not find valid resources", "no route found"],
        )
        phase6_ok = len(log_errors) == 0

        # ── Summary ─────────────────────────────────────────────────────────
        print("\n" + "=" * 78)
        print("LINK CONTROL LIFECYCLE & ROUTING IMPACT REGRESSION TEST SUMMARY")
        print("=" * 78)
        print(f"{'CHECK':<44} {'STATUS':<8} {'DETAILS'}")
        print("-" * 78)
        print(
            f"{'1. Initial Convergence & Baseline EGP':<44} "
            f"{'OK' if phase1_ok else 'FAIL':<8} "
            f"converged={converged}, egp_code={egp_base_code}"
        )
        print(
            f"{'2. Multi-Probe Telemetry (3x probe)':<44} "
            f"{'OK' if phase2_ok else 'FAIL':<8} "
            f"tx_delta={tx_delta}, rx_delta={rx_delta}, hist={tx_hist_len}/{rx_hist_len}"
        )
        print(
            f"{'3. Admin Disconnect -> EGP Route Rejection':<44} "
            f"{'OK' if phase3_ok else 'FAIL':<8} "
            f"down={down_reached}, egp_code={egp_down_code}"
        )
        print(
            f"{'4. Admin Connect + Probe -> EGP Recovery':<44} "
            f"{'OK' if phase4_ok else 'FAIL':<8} "
            f"qup={qup_reached}, egp_code={egp_rec_code}"
        )
        print(
            f"{'5. Admin Parameter Tuning (action=set)':<44} "
            f"{'OK' if phase5_ok else 'FAIL':<8} "
            f"hold_time={hold_val}, hello=ok"
        )
        print(
            f"{'6. Log Cleanliness':<44} "
            f"{'OK' if phase6_ok else 'FAIL':<8} "
            f"{'clean' if phase6_ok else f'{len(log_errors)} error(s)'}"
        )
        print("=" * 78)

        if log_errors:
            print("\nLog errors found:")
            for name, line in log_errors[:10]:
                print(f"  {name}: {line}")

        return (
            phase1_ok
            and phase2_ok
            and phase3_ok
            and phase4_ok
            and phase5_ok
            and phase6_ok
        )

    finally:
        if rpc_client is not None:
            try:
                await rpc_client.stop()
            except Exception:
                pass
        if msg_server is not None:
            try:
                await msg_server.stop()
            except Exception:
                pass
        terminate_all(procs)


if __name__ == "__main__":
    try:
        ok = asyncio.run(main())
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
