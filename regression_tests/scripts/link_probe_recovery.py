#!/usr/bin/env python
"""
Quantum Probe Optical Impairment & Recovery Regression Test.

Starts the controller (with QUANTNET_LINK_ADJACENCY=true) and 5 agents
(LBNL-Q, UCB-Q, LBNL-SWITCH, UCB-SWITCH, LBNL-BSM) where UCB-SWITCH initially
starts with an impaired DummySignalMeasurement (efficiency=0.0, window=0.2),
and verifies four phases:

Phase 1 (Impaired Convergence & Probe Failure Telemetry):
  - 4 healthy quantum links reach QUANTUM_UP and 9 classical links reach CONTROL_UP.
  - 2 quantum links terminating on UCB-SWITCH (UCB-Q -> UCB-SWITCH ch 1 and
    LBNL-SWITCH -> UCB-SWITCH ch 99) reach CONTROL_UP via link.hello, fail UDP
    quantum probing, record probe_fail_count >= 1, and never reach QUANTUM_UP.

Phase 2 (Control-Up vs Quantum-Up Routing Rejection):
  - Sends an EGP request (LBNL-Q -> UCB-Q) while all 15 links are at least
    CONTROL_UP but the 2 UCB-SWITCH inbound quantum links are not QUANTUM_UP.
  - Verifies the controller rejects the EGP request because quantum channels
    strictly require QUANTUM_UP in ResourceManager.build_topology().

Phase 3 (Optical Detector Recovery & EGP Success):
  - Restarts UCB-SWITCH with a healthy DummySignalMeasurement (efficiency=1.0),
    re-probes UCB-Q -> UCB-SWITCH:1 and LBNL-SWITCH -> UCB-SWITCH:99, and
    verifies all 6 quantum links reach QUANTUM_UP.
  - Confirms UCB-Q's link telemetry for UCB-SWITCH:1 records both prior
    probe_fail_count >= 1 and post-recovery probe_ok_count >= 1.
  - Sends an EGP request (LBNL-Q -> UCB-Q) and verifies it now succeeds (OK).

Phase 4 (Log Cleanliness):
  - Verifies no unexpected Tracebacks, Errno, FileNotFoundError, or
    InvalidStateError appear in controller or agent logs.

Usage:
  python -u regression_tests/scripts/link_probe_recovery.py
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

HEALTHY_QUANTUM_LINKS = {
    ("LBNL-Q", "LBNL-SWITCH", "1"): "QUANTUM_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "14"): "QUANTUM_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "15"): "QUANTUM_UP",
    ("UCB-SWITCH", "LBNL-SWITCH", "100"): "QUANTUM_UP",
}

IMPAIRED_QUANTUM_LINKS = {
    ("UCB-Q", "UCB-SWITCH", "1"): "CONTROL_UP",
    ("LBNL-SWITCH", "UCB-SWITCH", "99"): "CONTROL_UP",
}

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
        prefix="ctrl_probe_rec_", suffix=".log", delete=False
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


def start_agent(agent_id, node_config, log_files, impair_detector=False, log_key=None):
    """Start a quantnet agent as a subprocess."""
    key = log_key or agent_id
    log_file = tempfile.NamedTemporaryFile(
        prefix=f"agent_{key}_", suffix=".log", delete=False
    )
    log_file.close()
    log_files[key] = log_file.name

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


def terminate_proc(proc):
    """Terminate a single subprocess cleanly."""
    try:
        proc.terminate()
    except Exception:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def terminate_all(procs):
    """Terminate all subprocesses cleanly."""
    for proc in procs.values():
        try:
            proc.terminate()
        except Exception:
            pass
    for proc in procs.values():
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


class RecoveryMonitor:
    """Tracks linkStateUpdate events on the MQTT monitor topic."""

    def __init__(self, known_cids):
        self.known_cids = set(known_cids)
        self.observed = {}
        self.quantum_up_events = []

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
            if state == "QUANTUM_UP":
                self.quantum_up_events.append(key)
        except Exception:
            return

    async def wait_for_states(self, target_map, timeout=60.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all(self.observed.get(k) == v for k, v in target_map.items()):
                return True
            await asyncio.sleep(0.3)
        return all(self.observed.get(k) == v for k, v in target_map.items())


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
    client = RPCClient(f"egp-recovery-{int(time.time() * 1000)}", host=host)
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
    procs = {}
    log_files = {}
    msg_server = None
    rpc_client = None

    try:
        # 1. Start MQTT monitor subscriber first
        monitor = RecoveryMonitor(AGENTS.keys())
        msg_server = MsgServer(host="localhost", port=1883)
        msg_server.subscribe("monitor", monitor.on_message)
        await msg_server.start()

        # 2. Start controller (with QUANTNET_LINK_ADJACENCY=true)
        log.info("Starting controller (QUANTNET_LINK_ADJACENCY=true)...")
        procs["controller"] = start_controller(log_files)
        await asyncio.sleep(4)

        # 3. Start 5 agents — UCB-SWITCH starts with impaired detector (efficiency=0.0)
        log.info("Starting 5 agents (UCB-SWITCH with impaired detector efficiency=0.0)...")
        topo_dir = os.path.join(
            WORKSPACE, "qn-mq", "quantnet_mq", "schema", "examples", "topology"
        )
        for agent_id, cfg_file in AGENTS.items():
            node_cfg = os.path.join(topo_dir, cfg_file)
            impair = agent_id == "UCB-SWITCH"
            log_key = "UCB-SWITCH_impaired" if impair else agent_id
            procs[agent_id] = start_agent(
                agent_id, node_cfg, log_files, impair_detector=impair, log_key=log_key
            )
            await asyncio.sleep(0.5)

        # ── Phase 1: Impaired Convergence & Probe Failure Telemetry ─────────
        log.info("\n" + "=" * 70)
        log.info("PHASE 1: Waiting for healthy QUANTUM_UP and impaired CONTROL_UP...")
        phase1_targets = {}
        phase1_targets.update(HEALTHY_QUANTUM_LINKS)
        phase1_targets.update(IMPAIRED_QUANTUM_LINKS)
        phase1_targets.update(CLASSICAL_LINKS)

        phase1_converged = await monitor.wait_for_states(phase1_targets, timeout=60.0)
        await asyncio.sleep(4.0)

        rpc_client = RPCClient(f"recovery-ctrl-{os.getpid()}", host="localhost")
        rpc_client.set_handler(
            "link.control",
            None,
            "quantnet_mq.schema.models.link_adjacency.linkControl",
        )
        await rpc_client.start()

        # Trigger explicit on-demand probes on the two impaired links
        await call_link_control(
            rpc_client, "UCB-Q", action="probe", target="UCB-SWITCH:1"
        )
        await call_link_control(
            rpc_client, "LBNL-SWITCH", action="probe", target="UCB-SWITCH:99"
        )

        ucb_q_detail = await call_link_control(
            rpc_client, "UCB-Q", action="show_detail", target="UCB-SWITCH:1"
        )
        ucb_q_entry = ucb_q_detail.get("data") or {}
        fail_count_p1 = ucb_q_entry.get("probe_fail_count", 0)
        state_p1 = ucb_q_entry.get("state")

        never_quantum_up = all(
            k not in monitor.quantum_up_events for k in IMPAIRED_QUANTUM_LINKS
        )
        phase1_ok = (
            phase1_converged
            and never_quantum_up
            and state_p1 == "CONTROL_UP"
            and fail_count_p1 >= 1
        )
        log.info(
            f"Phase 1: converged={phase1_converged}, never_qup={never_quantum_up}, "
            f"UCB-Q->UCB-SWITCH:1 state={state_p1}, fail_count={fail_count_p1}"
        )

        # ── Phase 2: EGP Rejected When Quantum Links Are Only CONTROL_UP ────
        log.info("\n" + "=" * 70)
        log.info("PHASE 2: Verifying EGP is rejected while quantum links are CONTROL_UP...")
        egp_impaired_resp = await call_egp()
        egp_impaired_code = egp_impaired_resp.get("status", {}).get("code")
        egp_impaired_msg = egp_impaired_resp.get("status", {}).get("message", "")
        phase2_ok = egp_impaired_code in (
            Code.INVALID_ARGUMENT.value,
            Code.FAILED.value,
        )
        log.info(
            f"Phase 2: EGP code={egp_impaired_code} (msg={egp_impaired_msg})"
        )

        # ── Phase 3: Optical Detector Recovery & Re-Probe to QUANTUM_UP ─────
        log.info("\n" + "=" * 70)
        log.info("PHASE 3: Restarting UCB-SWITCH with healthy detector (efficiency=1.0)...")
        terminate_proc(procs["UCB-SWITCH"])
        await asyncio.sleep(1.0)

        ucb_sw_cfg = os.path.join(topo_dir, AGENTS["UCB-SWITCH"])
        procs["UCB-SWITCH"] = start_agent(
            "UCB-SWITCH",
            ucb_sw_cfg,
            log_files,
            impair_detector=False,
            log_key="UCB-SWITCH_healthy",
        )

        # Wait for UCB-SWITCH links to re-establish CONTROL_UP or QUANTUM_UP
        await asyncio.sleep(8.0)

        # Trigger explicit probes from UCB-Q and LBNL-SWITCH to accelerate convergence
        await call_link_control(
            rpc_client, "UCB-Q", action="probe", target="UCB-SWITCH:1"
        )
        await call_link_control(
            rpc_client, "LBNL-SWITCH", action="probe", target="UCB-SWITCH:99"
        )

        all_healthy_targets = {}
        all_healthy_targets.update(HEALTHY_QUANTUM_LINKS)
        for k in IMPAIRED_QUANTUM_LINKS:
            all_healthy_targets[k] = "QUANTUM_UP"
        all_healthy_targets.update(CLASSICAL_LINKS)

        phase3_converged = await monitor.wait_for_states(
            all_healthy_targets, timeout=45.0
        )
        await asyncio.sleep(2.0)

        ucb_q_post = await call_link_control(
            rpc_client, "UCB-Q", action="show_detail", target="UCB-SWITCH:1"
        )
        post_entry = ucb_q_post.get("data") or {}
        fail_count_p3 = post_entry.get("probe_fail_count", 0)
        ok_count_p3 = post_entry.get("probe_ok_count", 0)
        state_p3 = post_entry.get("state")

        egp_recovered_resp = await call_egp()
        egp_recovered_code = egp_recovered_resp.get("status", {}).get("code")

        phase3_ok = (
            phase3_converged
            and state_p3 == "QUANTUM_UP"
            and fail_count_p3 >= 1
            and ok_count_p3 >= 1
            and egp_recovered_code == Code.OK.value
        )
        log.info(
            f"Phase 3: converged={phase3_converged}, state={state_p3}, "
            f"fail_count={fail_count_p3}, ok_count={ok_count_p3}, "
            f"egp_code={egp_recovered_code}"
        )

        # ── Phase 4: Log Cleanliness ────────────────────────────────────────
        log.info("\n" + "=" * 70)
        log.info("PHASE 4: Checking controller and agent logs...")
        log_errors = check_logs(
            log_files,
            exclude_patterns=["Could not find valid resources", "no route found"],
        )
        phase4_ok = len(log_errors) == 0

        # ── Summary ─────────────────────────────────────────────────────────
        print("\n" + "=" * 78)
        print("QUANTUM PROBE IMPAIRMENT & RECOVERY REGRESSION TEST SUMMARY")
        print("=" * 78)
        print(f"{'CHECK':<44} {'STATUS':<8} {'DETAILS'}")
        print("-" * 78)
        print(
            f"{'1. Impaired Detector Stays CONTROL_UP':<44} "
            f"{'OK' if phase1_ok else 'FAIL':<8} "
            f"state={state_p1}, fail_count={fail_count_p1}"
        )
        print(
            f"{'2. EGP Rejected When Quantum Only CONTROL_UP':<44} "
            f"{'OK' if phase2_ok else 'FAIL':<8} "
            f"egp_code={egp_impaired_code}"
        )
        print(
            f"{'3. Detector Recovery -> QUANTUM_UP & EGP OK':<44} "
            f"{'OK' if phase3_ok else 'FAIL':<8} "
            f"state={state_p3}, ok={ok_count_p3}/fail={fail_count_p3}, egp={egp_recovered_code}"
        )
        print(
            f"{'4. Log Cleanliness':<44} "
            f"{'OK' if phase4_ok else 'FAIL':<8} "
            f"{'clean' if phase4_ok else f'{len(log_errors)} error(s)'}"
        )
        print("=" * 78)

        if log_errors:
            print("\nLog errors found:")
            for name, line in log_errors[:10]:
                print(f"  {name}: {line}")

        return phase1_ok and phase2_ok and phase3_ok and phase4_ok

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
