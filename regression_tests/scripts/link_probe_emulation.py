#!/usr/bin/env python
"""
Quantum Probe Emulation Regression Test (DummyLightSrc + DummySignalMeasurement over UDP).

Starts the controller (with QUANTNET_LINK_ADJACENCY=true) and 5 agents
(LBNL-Q, UCB-Q, LBNL-SWITCH, UCB-SWITCH, LBNL-BSM) as local subprocesses and
runs four verification phases:

Phase 1 (Strict Convergence):
  Unlike link_adjacency_minimal.py / link_adjacency.py (which allow CONTROL_UP
  to satisfy QUANTUM_UP), this test strictly requires all 6 active quantum
  channels to reach QUANTUM_UP via UDP virtual link probing while all 9
  classical channels remain at CONTROL_UP.

Phase 2 (Driver & Time-Tagger Telemetry):
  Inspects agent logs to verify that DummyLightSrc initialized outbound virtual
  links and set wavelength/power, and DummySignalMeasurement initialized
  inbound virtual links and LinkInterpreter verified the incoming quantum probes.

Phase 3 (Administrative link.control RPC & On-Demand Probing):
  Exercises link.control RPC on LBNL-Q (topic link/LBNL-Q) for target
  "LBNL-SWITCH:1":
    - show_detail: verifies state is QUANTUM_UP
    - disconnect: verifies link transitions to DOWN on monitor topic
    - connect + probe: triggers recovery and explicit on-demand UDP quantum
      probe, verifying the link returns to QUANTUM_UP

Phase 4 (Log Cleanliness):
  Scans controller and agent logs for Tracebacks, Errno, FileNotFoundError, or
  InvalidStateError.

Usage:
  python -u regression_tests/scripts/link_probe_emulation.py
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from quantnet_mq.msgserver import MsgServer
from quantnet_mq.rpcclient import RPCClient

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

# Strict expected link states for all 15 links among the 5 agents.
# Quantum links MUST reach QUANTUM_UP (CONTROL_UP is NOT accepted as final).
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
    log_file = tempfile.NamedTemporaryFile(prefix="ctrl_probe_", suffix=".log", delete=False)
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


def check_logs(log_files):
    """Scan log files for error patterns."""
    error_patterns = ["Traceback", "FileNotFoundError", "Errno", "InvalidStateError"]
    false_positives = ["RPC timeout", "--no-repl", "Cannot load local task"]

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
            log.warning("Could not read log %s: %s", path, e)
    return errors


def verify_driver_telemetry(log_files):
    """Verify DummyLightSrc and DummySignalMeasurement telemetry in agent logs."""
    checks = []

    def file_contains(agent_id, substring):
        path = log_files.get(agent_id)
        if not path or not os.path.exists(path):
            return False
        with open(path, "r") as f:
            return substring in f.read()

    checks.append((
        "LBNL-Q DummyLightSrc init",
        file_contains("LBNL-Q", "Initializing DummyLightSrc"),
    ))
    checks.append((
        "LBNL-Q wavelength/power config",
        file_contains("LBNL-Q", "Setting wavelength to 1550.0")
        and file_contains("LBNL-Q", "Setting power to 12.1"),
    ))
    checks.append((
        "LBNL-SWITCH DummySignalMeasurement init",
        file_contains("LBNL-SWITCH", "Initializing DummySignalMeasurement time-tagger"),
    ))
    checks.append((
        "LBNL-SWITCH probe verification",
        file_contains("LBNL-SWITCH", "LinkInterpreter: received probe from LBNL-Q ch 1"),
    ))
    checks.append((
        "UCB-SWITCH probe verification",
        file_contains("UCB-SWITCH", "LinkInterpreter: received probe from UCB-Q ch 1"),
    ))

    failed = [name for name, ok in checks if not ok]
    return len(failed) == 0, checks


class StrictLinkMonitor:
    """Subscribe to monitor topic and track link state transitions."""

    def __init__(self, known_cids):
        self.states = {}
        self.history = {}
        self.known_cids = known_cids

    async def on_message(self, msg):
        try:
            data = json.loads(msg)
            if data.get("eventType") != "linkStateUpdate":
                return
            value = data["value"]
            src = value["src_cid"]
            if src not in self.known_cids:
                return
            key = (src, value["dst_cid"], str(value["channel_id"]))
            state = value["state"]
            self.states[key] = state
            self.history.setdefault(key, []).append(state)
        except Exception as e:
            log.debug("Error processing monitor message: %s", e)

    def check_strict_convergence(self):
        """Require exact match for every expected link (CONTROL_UP does NOT satisfy QUANTUM_UP)."""
        mismatches = []
        for key, exp_state in STRICT_EXPECTED_LINKS.items():
            actual = self.states.get(key)
            if actual != exp_state:
                mismatches.append((key, exp_state, actual))
        return len(mismatches) == 0, mismatches


async def exercise_link_control_rpc(monitor):
    """Exercise link.control show_detail, disconnect, connect, and probe on LBNL-Q."""
    client = RPCClient(f"link-ctrl-test-{os.getpid()}", host="localhost")
    client.set_handler(
        "link.control",
        None,
        "quantnet_mq.schema.models.link_adjacency.linkControl",
    )
    await client.start()

    target = "LBNL-SWITCH:1"
    topic = "link/LBNL-Q"
    link_key = ("LBNL-Q", "LBNL-SWITCH", "1")

    # 1. show_detail (outbound on LBNL-Q and inbound on LBNL-SWITCH)
    resp = json.loads(
        await client.call(
            "link.control",
            {"action": "show_detail", "target": target},
            topic=topic,
            timeout=10.0,
        )
    )
    out_data = resp.get("data", {})
    if resp.get("status") != "ok" or out_data.get("state") != "QUANTUM_UP":
        return False, f"show_detail expected QUANTUM_UP, got {resp}"
    if out_data.get("probe_tx_count", 0) < 1 or out_data.get("probe_ok_count", 0) < 1:
        return False, f"outbound probe telemetry missing in show_detail: {out_data}"

    sw_resp = json.loads(
        await client.call(
            "link.control",
            {"action": "show_detail", "target": "LBNL-Q:1"},
            topic="link/LBNL-SWITCH",
            timeout=10.0,
        )
    )
    in_data = sw_resp.get("data", {})
    if (
        sw_resp.get("status") != "ok"
        or in_data.get("probe_rx_count", 0) < 1
        or not (in_data.get("last_probe_info") or {}).get("detected")
    ):
        return False, f"inbound time-tagger telemetry missing on LBNL-SWITCH: {sw_resp}"

    # 2. disconnect -> should transition link to DOWN
    resp = json.loads(
        await client.call(
            "link.control",
            {"action": "disconnect", "target": target},
            topic=topic,
            timeout=10.0,
        )
    )
    if resp.get("status") != "ok":
        return False, f"disconnect failed: {resp}"

    # Verify DOWN was observed via show_detail history or monitor
    await asyncio.sleep(0.5)
    detail_after_disc = json.loads(
        await client.call(
            "link.control",
            {"action": "show_detail", "target": target},
            topic=topic,
            timeout=10.0,
        )
    )
    hist_states = [
        h.get("state") for h in detail_after_disc.get("data", {}).get("history", [])
    ]
    if "DOWN" not in hist_states and "DOWN" not in monitor.history.get(link_key, []):
        return False, f"disconnect did not record DOWN transition: {hist_states}"

    # 3. connect -> initiate hello exchange
    resp = json.loads(
        await client.call(
            "link.control",
            {"action": "connect", "target": target},
            topic=topic,
            timeout=10.0,
        )
    )
    if resp.get("status") != "ok":
        return False, f"connect failed: {resp}"

    # 4. probe -> trigger explicit on-demand UDP quantum probe once link is up
    deadline = time.time() + 15.0
    while time.time() < deadline:
        probe_resp = json.loads(
            await client.call(
                "link.control",
                {"action": "probe", "target": target},
                topic=topic,
                timeout=10.0,
            )
        )
        if probe_resp.get("status") != "ok":
            return False, f"probe RPC failed: {probe_resp}"
        detail = json.loads(
            await client.call(
                "link.control",
                {"action": "show_detail", "target": target},
                topic=topic,
                timeout=10.0,
            )
        )
        if detail.get("data", {}).get("state") == "QUANTUM_UP":
            return True, "show_detail -> disconnect -> connect -> probe -> QUANTUM_UP"
        await asyncio.sleep(1.5)

    return False, "Link did not return to QUANTUM_UP after connect + probe"


async def main():
    procs = []
    log_files = {}
    try:
        monitor = StrictLinkMonitor(set(AGENTS.keys()))
        server = MsgServer(host="localhost")
        server.subscribe("monitor", monitor.on_message)
        await server.start()

        log.info("Starting controller...")
        procs.append(start_controller(log_files))
        await asyncio.sleep(3)

        topo_dir = os.path.join(
            WORKSPACE, "qn-mq", "quantnet_mq", "schema", "examples", "topology"
        )
        for agent_id, cfg_file in AGENTS.items():
            log.info("Starting agent %s...", agent_id)
            procs.append(start_agent(agent_id, os.path.join(topo_dir, cfg_file), log_files))
            await asyncio.sleep(0.4)

        # Phase 1: Wait for strict convergence (all 6 quantum links -> QUANTUM_UP)
        log.info("Phase 1: Waiting for strict QUANTUM_UP / CONTROL_UP convergence (timeout 60s)...")
        start_ts = time.time()
        converged = False
        while time.time() - start_ts < 60.0:
            await asyncio.sleep(2.0)
            converged, mismatches = monitor.check_strict_convergence()
            q_up = sum(
                1 for k, exp in STRICT_EXPECTED_LINKS.items()
                if exp == "QUANTUM_UP" and monitor.states.get(k) == "QUANTUM_UP"
            )
            c_up = sum(
                1 for k, exp in STRICT_EXPECTED_LINKS.items()
                if exp == "CONTROL_UP" and monitor.states.get(k) == "CONTROL_UP"
            )
            log.info(
                "  [%.0fs] Quantum QUANTUM_UP: %d/6 | Classical CONTROL_UP: %d/9",
                time.time() - start_ts,
                q_up,
                c_up,
            )
            if converged:
                break

        # Phase 2: Driver & Time-Tagger Telemetry
        log.info("Phase 2: Checking DummyLightSrc & DummySignalMeasurement log telemetry...")
        telemetry_ok, telemetry_checks = verify_driver_telemetry(log_files)

        # Phase 3: Administrative link.control RPC & On-Demand Probe
        log.info("Phase 3: Testing link.control RPC (show_detail, disconnect, connect, probe)...")
        rpc_ok, rpc_details = await exercise_link_control_rpc(monitor)

        # Phase 4: Log error scan
        log.info("Phase 4: Scanning logs for errors...")
        log_errors = check_logs(log_files)
        logs_clean = len(log_errors) == 0

        print("\n" + "=" * 75)
        print("QUANTUM PROBE EMULATION REGRESSION TEST (5 agents, UDP Virtual Links)")
        print("=" * 75)
        print(
            f"1. Strict Link Convergence (6 QUANTUM_UP, 9 CONTROL_UP): "
            f"{'PASS' if converged else 'FAIL'}"
        )
        if not converged:
            for k, exp, act in mismatches:
                print(f"     MISMATCH {k}: expected={exp}, actual={act}")
        print(
            f"2. Driver & Time-Tagger Telemetry:                      "
            f"{'PASS' if telemetry_ok else 'FAIL'}"
        )
        for name, ok in telemetry_checks:
            print(f"     [{'OK' if ok else 'FAIL'}] {name}")
        print(
            f"3. link.control RPC & On-Demand Probe:                  "
            f"{'PASS' if rpc_ok else 'FAIL'} ({rpc_details})"
        )
        print(
            f"4. Log Error Check:                                     "
            f"{'PASS' if logs_clean else 'FAIL'}"
        )
        if not logs_clean:
            for src, line in log_errors[:10]:
                print(f"     [{src}] {line}")
        print("=" * 75)

        overall = converged and telemetry_ok and rpc_ok and logs_clean
        sys.exit(0 if overall else 1)

    finally:
        log.info("Terminating subprocesses...")
        terminate_all(procs)


if __name__ == "__main__":
    asyncio.run(main())
