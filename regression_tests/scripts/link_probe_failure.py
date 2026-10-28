#!/usr/bin/env python
"""
Quantum Probe Failure / Optical Impairment Regression Test.

Starts the controller (with QUANTNET_LINK_ADJACENCY=true) and 5 agents
(LBNL-Q, UCB-Q, LBNL-SWITCH, UCB-SWITCH, LBNL-BSM) where UCB-SWITCH is
configured with an impaired DummySignalMeasurement (efficiency=0.0, window=0.2,
simulating a dead single-photon detector / broken inbound optical path) while
the MQTT control plane remains completely healthy.

Verifies:
1. Healthy Quantum & Classical Links Converge:
   - Healthy quantum links (LBNL-Q -> LBNL-SWITCH ch 1, LBNL-SWITCH -> LBNL-BSM
     ch 14 & 15, UCB-SWITCH -> LBNL-SWITCH ch 100) reach QUANTUM_UP.
   - All 9 classical links reach CONTROL_UP.
2. Impaired Quantum Links Stay at CONTROL_UP (Never Falsely QUANTUM_UP):
   - Inbound quantum channels on UCB-SWITCH (UCB-Q -> UCB-SWITCH ch 1 and
     LBNL-SWITCH -> UCB-SWITCH ch 99) reach CONTROL_UP via link.hello, attempt
     UDP quantum probing once IN_SPEC, are rejected by DummySignalMeasurement,
     and remain at CONTROL_UP across multiple probe intervals.
3. On-Demand link.control Probe Also Stays CONTROL_UP:
   - Calling link.control(action="probe", target="UCB-SWITCH:1") on UCB-Q
     followed by show_detail confirms state remains CONTROL_UP.
4. Log Telemetry & Cleanliness:
   - Confirms UCB-SWITCH logged "time-tagger did not detect quantum probe" and
     no process logged Tracebacks, Errno, FileNotFoundError, or InvalidStateError.

Usage:
  python -u regression_tests/scripts/link_probe_failure.py
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

# Healthy quantum links (inbound on LBNL-SWITCH and LBNL-BSM) must reach QUANTUM_UP
HEALTHY_QUANTUM_LINKS = {
    ("LBNL-Q", "LBNL-SWITCH", "1"): "QUANTUM_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "14"): "QUANTUM_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "15"): "QUANTUM_UP",
    ("UCB-SWITCH", "LBNL-SWITCH", "100"): "QUANTUM_UP",
}

# Impaired quantum links (terminating on UCB-SWITCH with efficiency=0.0) must
# reach CONTROL_UP via hello but MUST NOT reach QUANTUM_UP
IMPAIRED_QUANTUM_LINKS = {
    ("UCB-Q", "UCB-SWITCH", "1"): "CONTROL_UP",
    ("LBNL-SWITCH", "UCB-SWITCH", "99"): "CONTROL_UP",
}

# Classical links (all 9) reach CONTROL_UP
CLASSICAL_LINKS = {
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


def make_local_agent_cfg(workspace, template_cfg_path, impair_detector=False):
    """Read agent.cfg template, rewrite paths, and optionally impair DummySignalMeasurement."""
    with open(template_cfg_path, "r") as f:
        lines = f.readlines()

    workspace_cfg_path = os.path.join(workspace, "qn-agent", "config") + "/"
    new_lines = []
    in_sig_meas = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[[") and stripped.endswith("]]"):
            in_sig_meas = stripped == "[[signalmeasurement]]"
        if "path=" in line and "qn-agent/config/" in line:
            if "../qn-agent/config/" in line:
                line = line.replace("../qn-agent/config/", workspace_cfg_path)
            elif "/qn-agent/config/" in line:
                line = line.replace("/qn-agent/config/", workspace_cfg_path)
        if impair_detector and in_sig_meas and stripped.startswith("window="):
            new_lines.append("window=0.2\n")
            new_lines.append("efficiency=0.0\n")
            continue
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
        prefix="ctrl_probe_fail_", suffix=".log", delete=False
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


def start_agent(agent_id, node_config, log_files, impair_detector=False):
    """Start a quantnet agent as a subprocess."""
    log_file = tempfile.NamedTemporaryFile(
        prefix=f"agent_{agent_id}_", suffix=".log", delete=False
    )
    log_file.close()
    log_files[agent_id] = log_file.name

    local_agent_cfg = make_local_agent_cfg(
        WORKSPACE,
        os.path.join(WORKSPACE, "qn-agent", "config", "agent.cfg"),
        impair_detector=impair_detector,
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


class ImpairmentMonitor:
    """Subscribe to monitor topic and track link states and history."""

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

    def healthy_and_classical_ready(self):
        """Return True when all healthy quantum links are QUANTUM_UP and classical/impaired are CONTROL_UP."""
        for k, exp in HEALTHY_QUANTUM_LINKS.items():
            if self.states.get(k) != exp:
                return False
        for k, exp in CLASSICAL_LINKS.items():
            if self.states.get(k) != exp:
                return False
        for k in IMPAIRED_QUANTUM_LINKS:
            if self.states.get(k) is None:
                return False
        return True


async def verify_impaired_probe_rpc():
    """Call link.control probe + show_detail on UCB-Q for UCB-SWITCH:1 and confirm it stays CONTROL_UP."""
    client = RPCClient(f"link-fail-test-{os.getpid()}", host="localhost")
    client.set_handler(
        "link.control",
        None,
        "quantnet_mq.schema.models.link_adjacency.linkControl",
    )
    await client.start()

    target = "UCB-SWITCH:1"
    topic = "link/UCB-Q"

    probe_resp = json.loads(
        await client.call(
            "link.control",
            {"action": "probe", "target": target},
            topic=topic,
            timeout=10.0,
        )
    )
    if probe_resp.get("status") != "ok":
        return False, f"link.control probe call returned error: {probe_resp}"

    detail_resp = json.loads(
        await client.call(
            "link.control",
            {"action": "show_detail", "target": target},
            topic=topic,
            timeout=10.0,
        )
    )
    state = detail_resp.get("data", {}).get("state")
    if state != "CONTROL_UP":
        return False, f"Expected UCB-SWITCH:1 to remain CONTROL_UP, got {state}"
    return True, f"UCB-SWITCH:1 state={state} after on-demand probe"


async def main():
    procs = []
    log_files = {}
    try:
        monitor = ImpairmentMonitor(set(AGENTS.keys()))
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
            impair = agent_id == "UCB-SWITCH"
            log.info(
                "Starting agent %s (impair_detector=%s)...", agent_id, impair
            )
            procs.append(
                start_agent(
                    agent_id,
                    os.path.join(topo_dir, cfg_file),
                    log_files,
                    impair_detector=impair,
                )
            )
            await asyncio.sleep(0.4)

        log.info(
            "Waiting for healthy links to reach QUANTUM_UP/CONTROL_UP (timeout 60s)..."
        )
        start_ts = time.time()
        while time.time() - start_ts < 60.0:
            await asyncio.sleep(2.0)
            if monitor.healthy_and_classical_ready():
                break

        # Wait an extra 6s (2 hello/probe intervals) after IN_SPEC to ensure
        # UCB-Q and LBNL-SWITCH have attempted probing UCB-SWITCH multiple times
        log.info("Waiting 6s for probe attempts against impaired UCB-SWITCH detector...")
        await asyncio.sleep(6.0)

        # Verify healthy quantum links reached QUANTUM_UP
        healthy_ok = all(
            monitor.states.get(k) == "QUANTUM_UP" for k in HEALTHY_QUANTUM_LINKS
        )
        classical_ok = all(
            monitor.states.get(k) == "CONTROL_UP" for k in CLASSICAL_LINKS
        )

        # Verify impaired quantum links reached CONTROL_UP and NEVER reached QUANTUM_UP
        impaired_stay_control_up = all(
            monitor.states.get(k) == "CONTROL_UP"
            and "QUANTUM_UP" not in monitor.history.get(k, [])
            for k in IMPAIRED_QUANTUM_LINKS
        )

        # Verify on-demand link.control probe on UCB-Q also stays CONTROL_UP
        rpc_ok, rpc_details = await verify_impaired_probe_rpc()

        # Verify UCB-SWITCH log recorded time-tagger rejection
        ucb_sw_log = ""
        if "UCB-SWITCH" in log_files and os.path.exists(log_files["UCB-SWITCH"]):
            with open(log_files["UCB-SWITCH"], "r") as f:
                ucb_sw_log = f.read()
        rejection_logged = (
            "LinkInterpreter: time-tagger did not detect quantum probe" in ucb_sw_log
        )

        log_errors = check_logs(log_files)
        logs_clean = len(log_errors) == 0

        print("\n" + "=" * 75)
        print("QUANTUM PROBE FAILURE / DETECTOR IMPAIRMENT REGRESSION TEST")
        print("=" * 75)
        print(
            f"1. Healthy Quantum Links (4/4 QUANTUM_UP):              "
            f"{'PASS' if healthy_ok else 'FAIL'}"
        )
        print(
            f"2. Classical Links (9/9 CONTROL_UP):                    "
            f"{'PASS' if classical_ok else 'FAIL'}"
        )
        print(
            f"3. Impaired Links Stay CONTROL_UP (0 QUANTUM_UP):       "
            f"{'PASS' if impaired_stay_control_up else 'FAIL'}"
        )
        for k in IMPAIRED_QUANTUM_LINKS:
            print(
                f"     {k}: state={monitor.states.get(k)}, history={monitor.history.get(k, [])}"
            )
        print(
            f"4. On-Demand link.control Probe Stays CONTROL_UP:       "
            f"{'PASS' if rpc_ok else 'FAIL'} ({rpc_details})"
        )
        print(
            f"5. Time-Tagger Rejection Logged on UCB-SWITCH:          "
            f"{'PASS' if rejection_logged else 'FAIL'}"
        )
        print(
            f"6. Log Error Check:                                     "
            f"{'PASS' if logs_clean else 'FAIL'}"
        )
        if not logs_clean:
            for src, line in log_errors[:10]:
                print(f"     [{src}] {line}")
        print("=" * 75)

        overall = (
            healthy_ok
            and classical_ok
            and impaired_stay_control_up
            and rpc_ok
            and rejection_logged
            and logs_clean
        )
        sys.exit(0 if overall else 1)

    finally:
        log.info("Terminating subprocesses...")
        terminate_all(procs)


if __name__ == "__main__":
    asyncio.run(main())
