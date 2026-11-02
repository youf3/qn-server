#!/usr/bin/env python
"""
Comprehensive end-to-end regression test covering all Quant-Net features.

Starts the controller (with QUANTNET_LINK_ADJACENCY=true) and all 8 agents
as local Python subprocesses, subscribes to MQTT monitor and experiment_data
topics, and verifies in a single integrated run:
  1. Pre-convergence routing gate (EGP rejected before link adjacency converges)
  2. Link adjacency convergence across the 8-agent topology + monitor telemetry
  3. Probe monitor & link.control RPC (show, show_detail, on-demand quantum
     probe, parameter set, administrative disconnect -> DOWN -> connect/probe
     recovery)
  4. Topology & node discovery via controller getinfo RPC
  5. PingPong plugin (pingpong RPC + pong-<token> MQTT records)
  6. SPG plugin (spgRequest + spgQuery RPC)
  7. EGP plugin (egpRequest + egpQuery RPC)
  8. DQC plugin (dqcRequest RPC with grover_4_2qpu.txt)
  9. Link calibration protocol (calibrate, get, getLast RPCs)
  10. Recorded polarization tracking data replay (26 MQTT experiment_data
      samples, 6 channels, disturbance peak > 0.20, final sample < 0.1)
  11. Invalid link calibration path rejection (LBNL-Q -> UCB-Q without BSMNode)
  12. Multi-agent stop & restart resilience (stop UCB-Q, LBNL-BSM, UCB-BSM ->
      verify neighbor link degradation & EGP routing rejection while stopped ->
      restart agents -> verify link adjacency re-convergence -> verify PingPong,
      EGP, and BSM plugins succeed across restarted agents)
  13. Subprocess health (all 9 processes alive) and log cleanliness

Prerequisites:
  - mosquitto running on localhost:1883
  - mongo running on localhost:27017
  - workspace root at /Users/se-youngyu/quantum/

Usage:
  python -u regression_tests/scripts/full_feature_regression.py
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
import uuid
import pymongo
from quantnet_mq import Code
from quantnet_mq.msgserver import MsgServer
from quantnet_mq.rpcclient import RPCClient
from quantnet_mq.schema.models import Schema

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Resolve workspace root from this script's path
WORKSPACE = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))

# All 8 agents in the ESnet topology
AGENTS = {
    "LBNL-Q": "conf_lbnl-q.json",
    "UCB-Q": "conf_ucb-q.json",
    "LBNL-SWITCH": "conf_lbnl-switch.json",
    "UCB-SWITCH": "conf_ucb-switch.json",
    "LBNL-BSM": "conf_lbnl-bsm.json",
    "UCB-BSM": "conf_ucb-bsm.json",
    "LBNL-M": "conf_lbnl-m.json",
    "UCB-M": "conf_ucb-m.json",
}

# Core links required for routing EGP, DQC, BSM, and Link Calibration
CONVERGENCE_LINKS = {
    ("LBNL-Q", "LBNL-SWITCH", "1"): "QUANTUM_UP",
    ("LBNL-Q", "LBNL-SWITCH", "4"): "CONTROL_UP",
    ("UCB-Q", "UCB-SWITCH", "1"): "QUANTUM_UP",
    ("UCB-Q", "UCB-SWITCH", "4"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-Q", "2"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-Q", "3"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "11"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "13"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "14"): "QUANTUM_UP",
    ("LBNL-SWITCH", "LBNL-BSM", "15"): "QUANTUM_UP",
    ("LBNL-SWITCH", "UCB-SWITCH", "99"): "QUANTUM_UP",
    ("UCB-SWITCH", "UCB-Q", "2"): "CONTROL_UP",
    ("UCB-SWITCH", "UCB-Q", "3"): "CONTROL_UP",
    ("UCB-SWITCH", "LBNL-SWITCH", "100"): "QUANTUM_UP",
    ("LBNL-BSM", "LBNL-SWITCH", "2"): "CONTROL_UP",
}

EXPECTED_CHANNELS = ("Bob_H1", "Bob_D2", "Bob_H2", "Alice_H1", "Alice_D2", "Alice_H2")
EXPECTED_SAMPLE_COUNT = 26


def clean_mongodb(host="localhost", port=27017):
    """Drop non-system MongoDB databases to ensure a clean test state."""
    client = pymongo.MongoClient(host, port, serverSelectionTimeoutMS=5000)
    for db_name in client.list_database_names():
        if db_name not in ("admin", "config", "local"):
            client.drop_database(db_name)
    client.close()


def load_plugin_schemas():
    """Load all plugin schemas once into quantnet_mq Schema registry."""
    schema_dir = os.path.join(WORKSPACE, "qn-plugins", "plugins", "schema")
    for name in ("pingpong", "spg", "egp", "dqc", "bsm"):
        Schema.load_schema(os.path.join(schema_dir, f"{name}.yaml"), ns=name)


def check_qnpack_available():
    """Return True if qnpack is importable."""
    try:
        import qnpack.dqc  # noqa: F401
        return True
    except ImportError:
        return False


def make_local_agent_cfg(workspace, template_cfg_path):
    """Read agent.cfg template and rewrite paths to workspace-absolute paths."""
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
        mode="w",
        prefix="agent_cfg_",
        suffix=".cfg",
        delete=False,
    )
    temp_file.writelines(new_lines)
    temp_file.close()
    return temp_file.name


def start_controller(log_files):
    """Start the quantnet controller with QUANTNET_LINK_ADJACENCY=true."""
    log_file = tempfile.NamedTemporaryFile(prefix="ctrl_full_", suffix=".log", delete=False)
    log_file.close()
    log_files.append(("controller", log_file.name))

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


def start_agent(agent_id, node_config, log_files, log_label=None):
    """Start a quantnet agent as a subprocess."""
    label = log_label or agent_id
    log_file = tempfile.NamedTemporaryFile(prefix=f"agent_{label}_", suffix=".log", delete=False)
    log_file.close()
    log_files.append((label, log_file.name))

    local_agent_cfg = make_local_agent_cfg(
        WORKSPACE,
        os.path.join(WORKSPACE, "qn-agent", "config", "agent.cfg"),
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
    """Scan log files for unexpected tracebacks or runtime errors."""
    if exclude_patterns is None:
        exclude_patterns = []

    error_patterns = ["Traceback", "FileNotFoundError", "Errno", "InvalidStateError"]
    false_positive_patterns = [
        "RPC timeout",
        "Timeout awaiting RPC response",
        "--no-repl",
        "Cannot load local task",
    ] + exclude_patterns

    errors = []
    for name, path in log_files:
        try:
            with open(path, "r") as f:
                for line in f:
                    line = line.rstrip()
                    if any(pattern in line for pattern in error_patterns):
                        if not any(fp in line for fp in false_positive_patterns):
                            errors.append((name, line))
        except Exception as e:
            log.warning(f"Could not read log {path}: {e}")
    return errors


class TelemetryCollector:
    """Subscribe to MQTT monitor and experiment_data topics."""

    def __init__(self, known_cids):
        self.known_cids = set(known_cids)
        self.link_states = {}
        self.link_events = []
        self.probe_events = []
        self.experiment_data = []

    async def on_monitor(self, raw_msg):
        try:
            data = json.loads(raw_msg) if isinstance(raw_msg, (str, bytes)) else raw_msg
            if data.get("eventType") != "linkStateUpdate":
                return
            val = data.get("value", {})
            src = val.get("src_cid")
            if src not in self.known_cids:
                return
            key = (src, val.get("dst_cid"), str(val.get("channel_id")))
            self.link_states[key] = val.get("state")
            self.link_events.append(val)
            if val.get("probe_tx_count", 0) > 0 or val.get("last_probe_status"):
                self.probe_events.append(val)
        except Exception as e:
            log.debug("Error parsing monitor message: %s", e)

    async def on_experiment_data(self, raw_msg):
        try:
            data = json.loads(raw_msg) if isinstance(raw_msg, (str, bytes)) else raw_msg
            self.experiment_data.append(data)
        except Exception as e:
            log.debug("Error parsing experiment_data message: %s", e)

    def check_convergence(self, expected):
        failures = []
        for key, exp_state in expected.items():
            actual = self.link_states.get(key)
            if actual == exp_state:
                continue
            if exp_state == "QUANTUM_UP" and actual == "CONTROL_UP":
                continue
            failures.append(key)
        return len(failures) == 0, failures

    def exp_messages_for(self, expid):
        return [m for m in self.experiment_data if m.get("expid") == expid]


async def test_link_control_and_probe(collector, host="localhost"):
    """
    Exercise link.control RPC on LBNL-Q:
      - show & show_detail on LBNL-SWITCH:1
      - on-demand quantum probe on LBNL-SWITCH:1
      - set hello-interval=5
      - disconnect -> verify DOWN -> connect + probe -> verify recovery
    """
    client = RPCClient(f"link-ctrl-{os.getpid()}", host=host)
    client.set_handler(
        "link.control",
        None,
        "quantnet_mq.schema.models.link_adjacency.linkControl",
    )
    await client.start()
    topic = "rpc/LBNL-Q"
    target = "LBNL-SWITCH:1"

    try:
        # 1. show + show_detail
        show_raw = await client.call("link.control", {"action": "show"}, topic=topic, timeout=10.0)
        show_resp = json.loads(show_raw)
        links = show_resp.get("data", {}).get("links", {})

        detail_raw = await client.call(
            "link.control",
            {"action": "show_detail", "target": target},
            topic=topic,
            timeout=10.0,
        )
        detail_resp = json.loads(detail_raw)
        detail_data = detail_resp.get("data", {})
        tx_before = detail_data.get("probe_tx_count", 0)
        show_ok = (
            show_resp.get("status") == "ok"
            and target in links
            and detail_resp.get("status") == "ok"
            and detail_data.get("state") in ("CONTROL_UP", "QUANTUM_UP")
            and detail_data.get("is_quantum") is True
        )
        show_details = (
            f"{len(links)} links, {target} state={detail_data.get('state')}, tx={tx_before}"
        )

        # 2. On-demand quantum probe
        probe_raw = await client.call(
            "link.control",
            {"action": "probe", "target": target},
            topic=topic,
            timeout=10.0,
        )
        probe_resp = json.loads(probe_raw)
        probe_data = probe_resp.get("data", {})

        detail_after_raw = await client.call(
            "link.control",
            {"action": "show_detail", "target": target},
            topic=topic,
            timeout=10.0,
        )
        detail_after = json.loads(detail_after_raw).get("data", {})
        tx_after = detail_after.get("probe_tx_count", 0)
        ok_after = detail_after.get("probe_ok_count", 0)

        probe_ok = (
            probe_resp.get("status") == "ok"
            and probe_data.get("status") == "ok"
            and probe_data.get("detected") is True
            and tx_after > tx_before
            and ok_after >= 1
        )
        probe_details = (
            f"status={probe_data.get('status')}, detected={probe_data.get('detected')}, "
            f"tx={tx_before}->{tx_after}, ok={ok_after}"
        )

        # 3. Set parameter + administrative disconnect -> connect + probe recovery
        set_raw = await client.call(
            "link.control",
            {"action": "set", "param": "hello-interval", "value": 5},
            topic=topic,
            timeout=10.0,
        )
        set_ok = json.loads(set_raw).get("status") == "ok"

        disc_raw = await client.call(
            "link.control",
            {"action": "disconnect", "target": target},
            topic=topic,
            timeout=10.0,
        )
        disc_ok = json.loads(disc_raw).get("status") == "ok"

        down_detail = json.loads(
            await client.call(
                "link.control",
                {"action": "show_detail", "target": target},
                topic=topic,
                timeout=10.0,
            )
        ).get("data", {})
        went_down = down_detail.get("state") == "DOWN"

        conn_raw = await client.call(
            "link.control",
            {"action": "connect", "target": target},
            topic=topic,
            timeout=10.0,
        )
        conn_ok = json.loads(conn_raw).get("status") == "ok"

        # Wait for bilateral hello confirmation to return link to CONTROL_UP/QUANTUM_UP
        recovered_state = "DOWN"
        for _ in range(15):
            await asyncio.sleep(1.0)
            await client.call(
                "link.control",
                {"action": "connect", "target": target},
                topic=topic,
                timeout=10.0,
            )
            cur = json.loads(
                await client.call(
                    "link.control",
                    {"action": "show_detail", "target": target},
                    topic=topic,
                    timeout=10.0,
                )
            ).get("data", {})
            recovered_state = cur.get("state", "DOWN")
            if recovered_state in ("CONTROL_UP", "QUANTUM_UP"):
                # Trigger probe to promote back to QUANTUM_UP immediately
                await client.call(
                    "link.control",
                    {"action": "probe", "target": target},
                    topic=topic,
                    timeout=10.0,
                )
                cur = json.loads(
                    await client.call(
                        "link.control",
                        {"action": "show_detail", "target": target},
                        topic=topic,
                        timeout=10.0,
                    )
                ).get("data", {})
                recovered_state = cur.get("state", recovered_state)
                break

        admin_ok = (
            set_ok
            and disc_ok
            and went_down
            and conn_ok
            and recovered_state in ("CONTROL_UP", "QUANTUM_UP")
        )
        admin_details = (
            f"set={set_ok}, disconnect->DOWN={went_down}, recovered={recovered_state}"
        )

        return {
            "show_ok": show_ok,
            "show_details": show_details,
            "probe_ok": probe_ok,
            "probe_details": probe_details,
            "admin_ok": admin_ok,
            "admin_details": admin_details,
        }
    finally:
        await client.stop()


async def test_getinfo(host="localhost"):
    """Verify controller getinfo RPC for topology and node discovery."""
    client = RPCClient(f"getinfo-{os.getpid()}", host=host)
    client.set_handler("getinfo", None, "quantnet_mq.schema.models.getInfo")
    await client.start()
    try:
        topo_resp = json.loads(
            await client.call(
                "getinfo",
                {"type": "topology", "parameters": {"full": True}},
                timeout=15.0,
            )
        )
        node_resp = json.loads(
            await client.call(
                "getinfo",
                {"type": "node", "parameters": {}},
                timeout=15.0,
            )
        )
        nodes = node_resp.get("value", [])
        node_ids = {n.get("systemSettings", {}).get("ID") for n in nodes if isinstance(n, dict)}
        ok = (
            topo_resp.get("status", {}).get("code") == Code.OK.value
            and node_resp.get("status", {}).get("code") == Code.OK.value
            and set(AGENTS.keys()).issubset(node_ids)
        )
        return ok, f"{len(node_ids)}/{len(AGENTS)} agents registered in RM"
    finally:
        await client.stop()


async def test_pingpong(host="localhost"):
    """Verify PingPong plugin across LBNL-Q, UCB-Q, and LBNL-BSM."""
    dests = ["LBNL-Q", "UCB-Q", "LBNL-BSM"]
    iters = 2
    token = str(uuid.uuid4())
    pong_records = {}

    async def on_pong(raw_msg):
        data = json.loads(raw_msg) if isinstance(raw_msg, (str, bytes)) else raw_msg
        agent = data.get("agent")
        if agent:
            pong_records[agent] = data

    mserver = MsgServer(host=host)
    mserver.subscribe(f"pong-{token}", on_pong)
    await mserver.start()

    client = RPCClient(f"pingpong-{uuid.uuid4().hex[:8]}", host=host)
    client.set_handler("pingpong", None, "quantnet_mq.schema.models.pingpong.pingPongRequest")
    await client.start()
    try:
        resp = json.loads(
            await client.call(
                "pingpong",
                {"type": "ping", "destinations": dests, "iterations": iters, "token": token},
                timeout=20.0,
            )
        )
        if resp.get("status", {}).get("code") != Code.OK.value:
            return False, f"RPC failed: {resp}"

        for _ in range(20):
            if len(pong_records) >= len(dests):
                break
            await asyncio.sleep(0.5)

        all_ok = len(pong_records) == len(dests) and all(
            rec.get("phase") == "done_success" and rec.get("successes") == iters
            for rec in pong_records.values()
        )
        return (
            all_ok,
            f"{len(pong_records)}/{len(dests)} agents ponged ({iters}/{iters} iters)",
        )
    finally:
        await client.stop()
        await mserver.stop()


async def test_spg(host="localhost"):
    """Verify SPG plugin (spgRequest + spgQuery)."""
    client = RPCClient(f"spg-{uuid.uuid4().hex[:8]}", host=host)
    client.set_handler("spgRequest", None, "quantnet_mq.schema.models.spg.spgRequest")
    client.set_handler("spgQuery", None, "quantnet_mq.schema.models.spg.spgQuery")
    await client.start()
    try:
        req_resp = json.loads(
            await client.call(
                "spgRequest",
                {"nodes": ["LBNL-Q", "UCB-Q"], "rate": 100, "duration": 1},
                timeout=60.0,
            )
        )
        rid = req_resp.get("rid", "")
        query_resp = json.loads(
            await client.call("spgQuery", {"rid": rid}, timeout=20.0)
        ) if rid else {}
        ok = (
            req_resp.get("status", {}).get("code") == Code.OK.value
            and query_resp.get("status", {}).get("code") == Code.OK.value
        )
        return ok, f"rid={rid[:8]}... status={req_resp.get('status', {}).get('value')}"
    finally:
        await client.stop()


async def test_egp(host="localhost", expect_reject=False):
    """Verify EGP plugin (egpRequest + egpQuery)."""
    client = RPCClient(f"egp-{uuid.uuid4().hex[:8]}", host=host)
    client.set_handler("egpRequest", None, "quantnet_mq.schema.models.egp.egpRequest")
    client.set_handler("egpQuery", None, "quantnet_mq.schema.models.egp.egpQuery")
    await client.start()
    try:
        req_resp = json.loads(
            await client.call(
                "egpRequest",
                {
                    "source": "LBNL-Q",
                    "destination": "UCB-Q",
                    "pairs": 1,
                    "bellState": "∣Φ+⟩",
                    "fidelity": 0.9,
                },
                timeout=60.0,
            )
        )
        code = req_resp.get("status", {}).get("code")
        msg = req_resp.get("status", {}).get("message", "")
        if expect_reject:
            ok = code != Code.OK.value
            return ok, f"rejected as expected (code={code})"

        rid = req_resp.get("rid", "")
        query_resp = (
            json.loads(await client.call("egpQuery", {"rid": rid}, timeout=20.0))
            if rid
            else {}
        )
        ok = (
            code == Code.OK.value
            and query_resp.get("status", {}).get("code") == Code.OK.value
        )
        return ok, f"rid={rid[:8]}... {msg}"
    finally:
        await client.stop()


async def test_dqc(host="localhost"):
    """Verify DQC plugin (dqcRequest with grover_4_2qpu.txt)."""
    if not check_qnpack_available():
        return True, "SKIP (qnpack not installed)"

    circuit_path = os.path.join(
        WORKSPACE, "qnpack-public", "qnpack", "dqc", "commands", "grover_4_2qpu.txt"
    )
    if not os.path.exists(circuit_path):
        circuit_path = os.path.join(WORKSPACE, "qnpack", "dqc", "commands", "grover_4_2qpu.txt")

    client = RPCClient(f"dqc-{uuid.uuid4().hex[:8]}", host=host)
    client.set_handler("dqcRequest", None, "quantnet_mq.schema.models.dqc.dqcRequest")
    await client.start()
    try:
        resp = json.loads(
            await client.call(
                "dqcRequest",
                {
                    "circuit_mode": "tket",
                    "circuit_file": circuit_path,
                },
                timeout=120.0,
            )
        )
        code = resp.get("status", {}).get("code")
        ok = code == Code.OK.value
        return ok, f"rid={resp.get('rid', '')[:8]}... status={resp.get('status', {}).get('value')}"
    finally:
        await client.stop()


async def poll_calibration(client, cal_id, timeout=30.0, interval=1.0):
    """Poll calibrate(type='get', id=cal_id) until terminal state or timeout."""
    elapsed = 0.0
    last_record = None
    while elapsed < timeout:
        raw = await client.call(
            "calibrate",
            {"type": "get", "parameters": {"id": cal_id}},
            timeout=10.0,
        )
        resp = json.loads(raw)
        cals = resp.get("calibrations", [])
        if cals:
            rec = cals[0]
            last_record = rec
            code = rec.get("status", {}).get("code")
            result = rec.get("result", {})
            if code == Code.FAILED.value:
                return rec
            if code == Code.OK.value and result and "LBNL-BSM" in result:
                return rec
        await asyncio.sleep(interval)
        elapsed += interval
    return last_record


async def test_link_calibration(collector, host="localhost"):
    """Verify Link Calibration RPC (calibrate, get, getLast), MQTT replay, and invalid path."""
    client = RPCClient(f"cal-{os.getpid()}", host=host)
    client.set_handler("calibrate", None, "quantnet_mq.schema.models.agentCalibration")
    await client.start()
    try:
        req_msg = {
            "type": "calibrate",
            "parameters": {
                "type": 1,
                "src": "LBNL-Q",
                "dst": "LBNL-BSM",
                "power": 0.1,
                "cal_light": "H",
            },
        }
        submit_resp = json.loads(await client.call("calibrate", req_msg, timeout=30.0))
        cals = submit_resp.get("calibrations", [])
        if submit_resp.get("status", {}).get("code") != Code.OK.value or not cals:
            return {
                "cal_ok": False,
                "cal_details": f"submit failed: {submit_resp}",
                "stream_ok": False,
                "stream_details": "skipped",
                "invalid_ok": False,
                "invalid_details": "skipped",
            }

        cal_id = cals[0].get("id")
        rec = await poll_calibration(client, cal_id, timeout=30.0)
        await asyncio.sleep(1.0)

        # Also verify get (list all calibrations) returns our completed calibration
        list_resp = json.loads(
            await client.call("calibrate", {"type": "get", "parameters": {}}, timeout=10.0)
        )
        list_cals = list_resp.get("calibrations", [])
        get_list_ok = any(c.get("id") == cal_id for c in list_cals if isinstance(c, dict))

        cal_ok = False
        cal_details = "incomplete"
        if rec:
            status_code = rec.get("status", {}).get("code")
            bsm_entry = rec.get("result", {}).get("LBNL-BSM", {})
            src_entry = rec.get("result", {}).get("LBNL-Q", {})
            bsm_res = bsm_entry.get("result", {})
            if (
                status_code == Code.OK.value
                and src_entry.get("status", {}).get("code") == Code.OK.value
                and bsm_entry.get("status", {}).get("code") == Code.OK.value
                and bsm_res.get("status") == "done"
                and bsm_res.get("samples_replayed") == EXPECTED_SAMPLE_COUNT
                and bsm_res.get("stabilized") is True
                and get_list_ok
            ):
                cal_ok = True
                cal_details = f"id={cal_id[:8]}... stabilized=True, list={len(list_cals)}"
            else:
                cal_details = f"unexpected result: {rec}"

        # Verify MQTT experiment_data stream
        exp_msgs = collector.exp_messages_for(cal_id)
        stream_ok = False
        if len(exp_msgs) == EXPECTED_SAMPLE_COUNT:
            cids_ok = all(m.get("cid") == "LBNL-BSM" for m in exp_msgs)
            channels_ok = all(
                all(ch in m.get("data", {}).get("pol_tracking", {}) for ch in EXPECTED_CHANNELS)
                for m in exp_msgs
            )
            peak_alice_h2 = (
                max(m["data"]["pol_tracking"]["Alice_H2"] for m in exp_msgs) if channels_ok else 0.0
            )
            final_data = exp_msgs[-1].get("data", {}).get("pol_tracking", {})
            final_stabilized = channels_ok and all(final_data[ch] < 0.1 for ch in EXPECTED_CHANNELS)
            if cids_ok and channels_ok and peak_alice_h2 > 0.20 and final_stabilized:
                stream_ok = True
                stream_details = f"{len(exp_msgs)} msgs, peak Alice_H2={peak_alice_h2:.4f}, final < 0.1"
            else:
                stream_details = f"cids={cids_ok}, ch={channels_ok}, peak={peak_alice_h2:.4f}"
        else:
            stream_details = f"expected {EXPECTED_SAMPLE_COUNT} msgs, got {len(exp_msgs)}"

        # Invalid path rejection (LBNL-Q -> UCB-Q without BSMNode)
        bad_msg = {
            "type": "calibrate",
            "parameters": {
                "type": 1,
                "src": "LBNL-Q",
                "dst": "UCB-Q",
                "power": 0.1,
                "cal_light": "H",
            },
        }
        bad_submit = json.loads(await client.call("calibrate", bad_msg, timeout=30.0))
        bad_cals = bad_submit.get("calibrations", [])
        invalid_ok = False
        invalid_details = "submit did not return id"
        if bad_cals:
            bad_id = bad_cals[0].get("id")
            bad_rec = await poll_calibration(client, bad_id, timeout=15.0)
            if bad_rec and bad_rec.get("status", {}).get("code") == Code.FAILED.value:
                invalid_ok = True
                invalid_details = "gracefully rejected (Code.FAILED)"
            else:
                invalid_details = f"unexpected status: {bad_rec}"

        return {
            "cal_ok": cal_ok,
            "cal_details": cal_details,
            "stream_ok": stream_ok,
            "stream_details": stream_details,
            "invalid_ok": invalid_ok,
            "invalid_details": invalid_details,
        }
    finally:
        await client.stop()


async def test_bsm(host="localhost"):
    """Verify BSM plugin (bsmRequest + bsmQuery)."""
    client = RPCClient(f"bsm-{uuid.uuid4().hex[:8]}", host=host)
    client.set_handler("bsmRequest", None, "quantnet_mq.schema.models.bsm.bsmRequest")
    client.set_handler("bsmQuery", None, "quantnet_mq.schema.models.bsm.bsmQuery")
    await client.start()
    try:
        req_resp = json.loads(
            await client.call(
                "bsmRequest",
                {"nodes": ["LBNL-Q", "UCB-Q", "LBNL-BSM"]},
                timeout=60.0,
            )
        )
        rid = req_resp.get("rid", "")
        query_resp = (
            json.loads(await client.call("bsmQuery", {"rid": rid}, timeout=20.0))
            if rid
            else {}
        )
        ok = (
            req_resp.get("status", {}).get("code") == Code.OK.value
            and query_resp.get("status", {}).get("code") == Code.OK.value
        )
        return ok, f"rid={rid[:8]}... status={req_resp.get('status', {}).get('value')}"
    finally:
        await client.stop()


STOP_AGENTS = ["UCB-Q", "LBNL-BSM", "UCB-BSM"]

OUTAGE_WATCH_LINKS = [
    ("LBNL-SWITCH", "LBNL-BSM", "11"),
    ("LBNL-SWITCH", "LBNL-BSM", "13"),
    ("LBNL-SWITCH", "LBNL-BSM", "14"),
    ("LBNL-SWITCH", "LBNL-BSM", "15"),
    ("UCB-SWITCH", "UCB-Q", "2"),
    ("UCB-SWITCH", "UCB-Q", "3"),
]


async def test_agent_stop_and_restart(agent_procs, log_files, collector, topo_dir, host="localhost"):
    """Stop 3 agents (UCB-Q, LBNL-BSM, UCB-BSM) for a while, verify link outage & routing
    rejection, then restart them and verify full link re-convergence and plugin recovery.
    """
    client = RPCClient(f"stop-restart-{uuid.uuid4().hex[:8]}", host=host)
    client.set_handler(
        "link.control",
        None,
        "quantnet_mq.schema.models.link_adjacency.linkControl",
    )
    await client.start()
    try:
        # 1. Shorten hold-time on surviving switches so neighbor loss is detected quickly
        for sw in ("LBNL-SWITCH", "UCB-SWITCH"):
            await client.call(
                "link.control",
                {"action": "set", "param": "hello-interval", "value": 2},
                topic=f"rpc/{sw}",
                timeout=10.0,
            )
            await client.call(
                "link.control",
                {"action": "set", "param": "hold-time", "value": 8},
                topic=f"rpc/{sw}",
                timeout=10.0,
            )

        # 2. Stop UCB-Q, LBNL-BSM, and UCB-BSM
        log.info(f"Stopping agents {STOP_AGENTS}...")
        for aid in STOP_AGENTS:
            proc = agent_procs[aid]
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

        stopped_ok = all(agent_procs[aid].poll() is not None for aid in STOP_AGENTS)

        # Clear cached states for links involving stopped agents to observe fresh outage updates
        for key in list(collector.link_states.keys()):
            if key[0] in STOP_AGENTS or key[1] in STOP_AGENTS:
                del collector.link_states[key]

        # Trigger on-demand probes from LBNL-SWITCH toward stopped LBNL-BSM concurrently
        # so quantum links immediately detect RPC timeout (~5s) and demote from QUANTUM_UP
        await asyncio.gather(
            client.call(
                "link.control",
                {"action": "probe", "target": "LBNL-BSM:14"},
                topic="rpc/LBNL-SWITCH",
                timeout=15.0,
            ),
            client.call(
                "link.control",
                {"action": "probe", "target": "LBNL-BSM:15"},
                topic="rpc/LBNL-SWITCH",
                timeout=15.0,
            ),
        )

        # Wait while agents remain stopped so hold timers expire links to DOWN on switches
        down_count = 0
        demoted_count = 0
        for _ in range(25):
            down_count = sum(
                1 for lk in OUTAGE_WATCH_LINKS
                if collector.link_states.get(lk) == "DOWN"
            )
            demoted_count = sum(
                1 for lk in OUTAGE_WATCH_LINKS
                if collector.link_states.get(lk) in ("DOWN", "CONTROL_UP")
            )
            if down_count == len(OUTAGE_WATCH_LINKS):
                break
            await asyncio.sleep(1.0)

        outage_ok = stopped_ok and demoted_count >= 2 and down_count >= 2
        outage_details = (
            f"stopped={len(STOP_AGENTS)} ({','.join(STOP_AGENTS)}), "
            f"{down_count}/{len(OUTAGE_WATCH_LINKS)} DOWN ({demoted_count} demoted)"
        )

        # 3. Verify EGP is rejected while UCB-Q and LBNL-BSM are stopped
        outage_egp_ok, outage_egp_details = await test_egp(expect_reject=True, host=host)

        # 4. Restore standard hold-time on switches before restarting agents
        for sw in ("LBNL-SWITCH", "UCB-SWITCH"):
            await client.call(
                "link.control",
                {"action": "set", "param": "hold-time", "value": 30},
                topic=f"rpc/{sw}",
                timeout=10.0,
            )
            await client.call(
                "link.control",
                {"action": "set", "param": "hello-interval", "value": 3},
                topic=f"rpc/{sw}",
                timeout=10.0,
            )

        # Clear cached states for links involving stopped agents so we verify fresh UP events
        for key in list(collector.link_states.keys()):
            if key[0] in STOP_AGENTS or key[1] in STOP_AGENTS:
                del collector.link_states[key]

        # 5. Restart the 3 stopped agents with their real agent IDs
        log.info(f"Restarting agents {STOP_AGENTS}...")
        for aid in STOP_AGENTS:
            node_config = os.path.join(topo_dir, AGENTS[aid])
            agent_procs[aid] = start_agent(
                aid,
                node_config,
                log_files,
                log_label=f"{aid}-restarted",
            )
            await asyncio.sleep(0.4)

        # Wait 5s for restarted agents to register and start LinkAdjacencyManager
        await asyncio.sleep(5)
        for aid in STOP_AGENTS:
            try:
                await client.call(
                    "link.control",
                    {"action": "set", "param": "hello-interval", "value": 3},
                    topic=f"rpc/{aid}",
                    timeout=10.0,
                )
            except Exception:
                pass

        # 6. Wait for all 15 core links in CONVERGENCE_LINKS to re-converge
        log.info("Waiting for all 15 core links to re-converge after agent restart (timeout: 60s)...")
        reconverged = False
        failures = []
        elapsed = 0
        while elapsed < 60:
            await asyncio.sleep(2)
            elapsed += 2
            reconverged, failures = collector.check_convergence(CONVERGENCE_LINKS)
            n_ok = len(CONVERGENCE_LINKS) - len(failures)
            log.info(f"[restart +{elapsed:2d}s] {n_ok}/{len(CONVERGENCE_LINKS)} core links re-converged")
            if reconverged:
                break

        # Restore 5s hello-interval
        for node in ["LBNL-SWITCH", "UCB-SWITCH"] + STOP_AGENTS:
            try:
                await client.call(
                    "link.control",
                    {"action": "set", "param": "hello-interval", "value": 5},
                    topic=f"rpc/{node}",
                    timeout=10.0,
                )
            except Exception:
                pass

        # Allow agent status heartbeat (last_seen) to be processed by controller
        await asyncio.sleep(3)

        reconv_details = (
            f"{len(CONVERGENCE_LINKS) - len(failures)}/{len(CONVERGENCE_LINKS)} links up "
            f"in {elapsed}s"
        )

        # 7. Verify plugins across the restarted agents (PingPong + EGP)
        post_pp_ok, post_pp_details = await test_pingpong(host=host)
        post_egp_ok, post_egp_details = await test_egp(expect_reject=False, host=host)
        post_plugins_ok = post_pp_ok and post_egp_ok
        post_plugins_details = f"PingPong={'OK' if post_pp_ok else 'FAIL'}, EGP={post_egp_details}"

        return {
            "outage_ok": outage_ok,
            "outage_details": outage_details,
            "outage_egp_ok": outage_egp_ok,
            "outage_egp_details": outage_egp_details,
            "reconv_ok": reconverged,
            "reconv_details": reconv_details,
            "post_plugins_ok": post_plugins_ok,
            "post_plugins_details": post_plugins_details,
        }
    finally:
        await client.stop()


async def main():
    """Main test orchestration."""
    ctrl_proc = None
    agent_procs = {}
    log_files = []
    server = None
    summary_rows = []

    try:
        # 0. Clean MongoDB and load plugin schemas
        log.info("Cleaning MongoDB database...")
        clean_mongodb()
        load_plugin_schemas()

        # 1. Start controller with QUANTNET_LINK_ADJACENCY=true
        log.info("Starting controller (QUANTNET_LINK_ADJACENCY=true)...")
        ctrl_proc = start_controller(log_files)
        await asyncio.sleep(5)

        # 2. Subscribe to MQTT monitor and experiment_data topics
        collector = TelemetryCollector(AGENTS.keys())
        server = MsgServer(host="localhost")
        server.subscribe("monitor", collector.on_monitor)
        server.subscribe("experiment_data", collector.on_experiment_data)
        await server.start()

        # 3. Start all 8 agents
        log.info("Starting 8 agents...")
        topo_dir = os.path.join(WORKSPACE, "qn-mq", "quantnet_mq", "schema", "examples", "topology")
        for agent_id, config_file in AGENTS.items():
            node_config = os.path.join(topo_dir, config_file)
            agent_procs[agent_id] = start_agent(agent_id, node_config, log_files)
            await asyncio.sleep(0.4)

        # 4. Wait 5s for registration, then test pre-convergence routing gate
        log.info("Waiting 5s for initial agent registration...")
        await asyncio.sleep(5)
        log.info("Phase 1: Testing pre-convergence EGP routing gate...")
        early_egp_ok, early_egp_details = await test_egp(expect_reject=True)
        summary_rows.append(("1. Pre-Convergence Gate", early_egp_ok, early_egp_details))

        # 5. Wait for link adjacency convergence across CONVERGENCE_LINKS
        log.info("Phase 2: Waiting for link adjacency convergence (timeout: 60s)...")
        timeout = 60
        interval = 2
        elapsed = 0
        converged = False
        while elapsed < timeout:
            await asyncio.sleep(interval)
            elapsed += interval
            is_conv, failures = collector.check_convergence(CONVERGENCE_LINKS)
            n_ok = len(CONVERGENCE_LINKS) - len(failures)
            log.info(f"[{elapsed:2d}s] {n_ok}/{len(CONVERGENCE_LINKS)} core links converged")
            if is_conv:
                converged = True
                break

        conv_details = (
            f"{len(CONVERGENCE_LINKS) - len(failures)}/{len(CONVERGENCE_LINKS)} links up, "
            f"{len(collector.probe_events)} probe events"
        )
        summary_rows.append(("2. Link Adjacency & Monitor", converged, conv_details))

        # Ensure first agent status heartbeat (last_seen) has been processed
        await asyncio.sleep(3)

        # 6. Exercise link.control RPC and on-demand quantum probe
        log.info("Phase 3: Testing link.control RPC & on-demand quantum probe...")
        lc_res = await test_link_control_and_probe(collector)
        summary_rows.append(("3. Link Control Show/Detail", lc_res["show_ok"], lc_res["show_details"]))
        summary_rows.append(("4. On-Demand Quantum Probe", lc_res["probe_ok"], lc_res["probe_details"]))
        summary_rows.append(("5. Link Admin Flap/Recover", lc_res["admin_ok"], lc_res["admin_details"]))

        # 7. Test topology & node discovery via getinfo
        log.info("Phase 4: Testing controller getinfo (topology & nodes)...")
        info_ok, info_details = await test_getinfo()
        summary_rows.append(("6. Topology & Node getinfo", info_ok, info_details))

        # 8. Test PingPong plugin
        log.info("Phase 5: Testing PingPong plugin...")
        pp_ok, pp_details = await test_pingpong()
        summary_rows.append(("7. PingPong Plugin", pp_ok, pp_details))

        # 9. Test SPG plugin
        log.info("Phase 6: Testing SPG plugin (spgRequest + spgQuery)...")
        spg_ok, spg_details = await test_spg()
        summary_rows.append(("8. SPG Plugin", spg_ok, spg_details))

        # 10. Test EGP plugin (post-convergence)
        log.info("Phase 7: Testing EGP plugin (egpRequest + egpQuery)...")
        egp_ok, egp_details = await test_egp(expect_reject=False)
        summary_rows.append(("9. EGP Plugin", egp_ok, egp_details))

        # 11. Test DQC plugin
        log.info("Phase 8: Testing DQC plugin (dqcRequest)...")
        dqc_ok, dqc_details = await test_dqc()
        summary_rows.append(("10. DQC Plugin", dqc_ok, dqc_details))

        # 12. Test Link Calibration + Recorded Data Replay + Invalid Path Rejection
        log.info("Phase 9: Testing Link Calibration & Recorded PolTracking Replay...")
        cal_res = await test_link_calibration(collector)
        summary_rows.append(("11. Link Calibration RPC", cal_res["cal_ok"], cal_res["cal_details"]))
        summary_rows.append(("12. PolTracking MQTT Replay", cal_res["stream_ok"], cal_res["stream_details"]))
        summary_rows.append(("13. Cal Invalid Path Reject", cal_res["invalid_ok"], cal_res["invalid_details"]))

        # 13. Stop 3 agents (UCB-Q, LBNL-BSM, UCB-BSM), verify outage & routing gate,
        #     then restart them and verify full re-convergence and post-restart plugins
        log.info("Phase 10: Testing multi-agent stop, outage detection, and restart recovery...")
        sr_res = await test_agent_stop_and_restart(agent_procs, log_files, collector, topo_dir)
        summary_rows.append(("14. Agent Stop & Link Outage", sr_res["outage_ok"], sr_res["outage_details"]))
        summary_rows.append(("15. Outage EGP Route Gate", sr_res["outage_egp_ok"], sr_res["outage_egp_details"]))
        summary_rows.append(("16. Agent Restart Re-Converge", sr_res["reconv_ok"], sr_res["reconv_details"]))
        summary_rows.append(
            ("17. Post-Restart PingPong+EGP", sr_res["post_plugins_ok"], sr_res["post_plugins_details"])
        )

        # 14. Test BSM plugin (also exercises restarted UCB-Q & LBNL-BSM; reserves 60s of slots)
        log.info("Phase 11: Testing BSM plugin on restarted agents (bsmRequest + bsmQuery)...")
        bsm_ok, bsm_details = await test_bsm()
        summary_rows.append(("18. BSM Plugin (Post-Restart)", bsm_ok, bsm_details))

        # 15. Check subprocess health and logs
        active_procs = ([ctrl_proc] if ctrl_proc else []) + list(agent_procs.values())
        procs_ok = all(proc.poll() is None for proc in active_procs)
        procs_details = f"{sum(1 for p in active_procs if p.poll() is None)}/{len(active_procs)} processes alive"
        summary_rows.append(("19. Subprocesses Alive", procs_ok, procs_details))

        log_errors = check_logs(
            log_files,
            exclude_patterns=[
                "no route found",
                "Could not find valid resources",
                "No BSMNode found in path",
            ],
        )
        log_ok = len(log_errors) == 0
        log_details = "0 unexpected errors" if log_ok else f"{len(log_errors)} errors found"
        summary_rows.append(("20. Log Cleanliness", log_ok, log_details))

        # Print comprehensive summary table
        print("\n" + "=" * 88)
        print("QUANT-NET FULL FEATURE REGRESSION SUMMARY (8 Agents + Controller)")
        print("=" * 88)
        print(f"{'FEATURE / TEST PHASE':<30} {'STATUS':<10} {'DETAILS':<46}")
        print("-" * 88)
        for name, ok, details in summary_rows:
            status_str = "PASS" if ok else "FAIL"
            print(f"{name:<30} {status_str:<10} {details:<46}")
        print("=" * 88)

        if log_errors:
            print("\nUnexpected log errors:")
            for agent, line in log_errors[:10]:
                print(f"  {agent}: {line}")

        print("\nLog files:")
        for agent, path in log_files:
            print(f"  {agent}: {path}")

        return all(ok for _, ok, _ in summary_rows)

    finally:
        if server:
            try:
                await server.stop()
            except Exception:
                pass
        log.info("Terminating all subprocesses...")
        active_procs = ([ctrl_proc] if ctrl_proc else []) + list(agent_procs.values())
        terminate_all(active_procs)


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        log.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
