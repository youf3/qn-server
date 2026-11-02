#!/usr/bin/env python
"""
Self-contained link calibration regression test (5 agents).

Starts a controller and 5 agents (LBNL-Q, UCB-Q, LBNL-SWITCH, UCB-SWITCH,
LBNL-BSM) as local Python subprocesses, subscribes to the MQTT experiment_data
topic, and verifies:
  1. Valid link calibration (LBNL-Q -> LBNL-BSM) completes with Code.OK and
     stabilized=True.
  2. Recorded polarization tracking data (26 samples across 6 channels) is
     streamed over MQTT experiment_data with the expected disturbance peak
     and final stabilization (< 0.1).
  3. Invalid calibration path (LBNL-Q -> UCB-Q, missing BSMNode) is gracefully
     rejected with Code.FAILED.
  4. All processes stay alive and logs contain no unexpected errors.

Prerequisites:
  - mosquitto running on localhost:1883
  - mongo running on localhost:27017
  - workspace root at /Users/se-youngyu/quantum/

Usage:
  python -u regression_tests/scripts/link_calibration.py
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
import pymongo
from quantnet_mq import Code
from quantnet_mq.msgserver import MsgServer
from quantnet_mq.rpcclient import RPCClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# Resolve workspace root from this script's path
WORKSPACE = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))

# Agent topology config file mapping
AGENTS = {
    "LBNL-Q": "conf_lbnl-q.json",
    "UCB-Q": "conf_ucb-q.json",
    "LBNL-SWITCH": "conf_lbnl-switch.json",
    "UCB-SWITCH": "conf_ucb-switch.json",
    "LBNL-BSM": "conf_lbnl-bsm.json",
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


def make_local_agent_cfg(workspace, template_cfg_path):
    """
    Read agent.cfg template and rewrite Docker paths to workspace-absolute paths.
    """
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
    """Start the quantnet controller as a subprocess."""
    log_file = tempfile.NamedTemporaryFile(prefix="ctrl_", suffix=".log", delete=False)
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
    with open(log_file.name, "w") as f:
        proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
    return proc


def start_agent(agent_id, node_config, log_files):
    """Start a quantnet agent as a subprocess."""
    log_file = tempfile.NamedTemporaryFile(prefix=f"agent_{agent_id}_", suffix=".log", delete=False)
    log_file.close()
    log_files.append((agent_id, log_file.name))

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
    """Terminate all subprocesses, with SIGTERM then SIGKILL."""
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
    """
    Scan log files for error patterns.

    Returns list of (filename, line) tuples matching error patterns,
    excluding lines with false positive patterns.
    """
    if exclude_patterns is None:
        exclude_patterns = []

    error_patterns = ["Traceback", "FileNotFoundError", "Errno", "InvalidStateError"]
    false_positive_patterns = ["RPC timeout", "--no-repl", "Cannot load local task"] + exclude_patterns

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


class ExperimentDataCollector:
    """Subscribe to MQTT experiment_data topic and collect messages by expid."""

    def __init__(self):
        self.messages = []

    async def on_message(self, raw_msg):
        try:
            data = json.loads(raw_msg) if isinstance(raw_msg, (str, bytes)) else raw_msg
            self.messages.append(data)
        except Exception as e:
            log.debug("Error parsing experiment_data message: %s", e)

    def for_expid(self, expid):
        return [m for m in self.messages if m.get("expid") == expid]


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


async def run_calibration_tests(collector, host="localhost"):
    """Run valid and invalid link calibration tests via RPC."""
    client = RPCClient(f"cal-test-{os.getpid()}", host=host)
    client.set_handler("calibrate", None, "quantnet_mq.schema.models.agentCalibration")
    await client.start()

    try:
        # --- Test 1: Valid Link Calibration (LBNL-Q -> LBNL-BSM) ---
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
        log.info(f"Submitting valid link calibration: {req_msg}")
        submit_resp = json.loads(await client.call("calibrate", req_msg, timeout=30.0))
        log.info(f"Submit response: {submit_resp}")

        cals = submit_resp.get("calibrations", [])
        if submit_resp.get("status", {}).get("code") != Code.OK.value or not cals:
            return {
                "cal_ok": False,
                "cal_details": f"submit failed: {submit_resp}",
                "stream_ok": False,
                "stream_details": "skipped (submit failed)",
                "invalid_ok": False,
                "invalid_details": "skipped",
            }

        cal_id = cals[0].get("id")
        rec = await poll_calibration(client, cal_id, timeout=30.0)
        log.info(f"Completed calibration record: {rec}")

        # Allow any in-flight MQTT experiment_data messages to arrive
        await asyncio.sleep(1.0)

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
            ):
                cal_ok = True
                cal_details = f"id={cal_id[:8]}... stabilized=True (26 samples)"
            else:
                cal_details = f"unexpected result: {rec}"

        # --- Test 2: Verify MQTT experiment_data telemetry stream ---
        exp_msgs = collector.for_expid(cal_id)
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
                stream_details = (
                    f"{len(exp_msgs)} msgs, peak Alice_H2={peak_alice_h2:.4f}, final < 0.1"
                )
            else:
                stream_details = (
                    f"cids_ok={cids_ok}, channels_ok={channels_ok}, "
                    f"peak={peak_alice_h2:.4f}, final_stabilized={final_stabilized}"
                )
        else:
            stream_details = f"expected {EXPECTED_SAMPLE_COUNT} msgs, got {len(exp_msgs)}"

        # --- Test 3: Invalid Calibration Path Rejection (LBNL-Q -> UCB-Q) ---
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
        log.info(f"Submitting invalid link calibration (missing BSMNode): {bad_msg}")
        bad_submit = json.loads(await client.call("calibrate", bad_msg, timeout=30.0))
        bad_cals = bad_submit.get("calibrations", [])
        invalid_ok = False
        invalid_details = "submit did not return id"
        if bad_cals:
            bad_id = bad_cals[0].get("id")
            bad_rec = await poll_calibration(client, bad_id, timeout=15.0)
            log.info(f"Invalid calibration record: {bad_rec}")
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


async def main():
    """Main test orchestration."""
    procs = []
    log_files = []
    server = None
    try:
        # 0. Clean MongoDB
        log.info("Cleaning MongoDB database...")
        clean_mongodb()

        # 1. Start controller
        log.info("Starting controller...")
        procs.append(start_controller(log_files))
        await asyncio.sleep(5)

        # 2. Start 5 agents
        log.info("Starting 5 agents...")
        topo_dir = os.path.join(WORKSPACE, "qn-mq", "quantnet_mq", "schema", "examples", "topology")
        for agent_id, config_file in AGENTS.items():
            node_config = os.path.join(topo_dir, config_file)
            procs.append(start_agent(agent_id, node_config, log_files))
            await asyncio.sleep(0.5)

        # 3. Subscribe to experiment_data topic
        collector = ExperimentDataCollector()
        server = MsgServer(host="localhost")
        server.subscribe("experiment_data", collector.on_message)
        await server.start()

        # 4. Wait for agent registration and first heartbeat (last_seen)
        log.info("Waiting 15s for agent registration and heartbeats...")
        await asyncio.sleep(15)

        # 5. Run calibration tests
        log.info("\n" + "=" * 70)
        log.info("Running Link Calibration regression tests...")
        log.info("=" * 70)
        results = await run_calibration_tests(collector)

        # 6. Check all subprocesses still alive
        procs_ok = all(proc.poll() is None for proc in procs)
        procs_status = "OK" if procs_ok else "FAIL"
        procs_details = f"{sum(1 for p in procs if p.poll() is None)}/{len(procs)} alive"

        # 7. Check logs
        log.info("Checking logs for errors...")
        log_errors = check_logs(log_files)
        log_ok = len(log_errors) == 0
        log_status = "OK" if log_ok else f"FAIL ({len(log_errors)} errors)"

        cal_status = "OK" if results["cal_ok"] else "FAIL"
        stream_status = "OK" if results["stream_ok"] else "FAIL"
        invalid_status = "OK" if results["invalid_ok"] else "FAIL"

        # 8. Print summary
        print("\n" + "=" * 75)
        print("LINK CALIBRATION REGRESSION — 5 agents (LBNL-Q -> LBNL-BSM)")
        print("=" * 75)
        print(f"{'TEST':<28} {'STATUS':<10} {'DETAILS':<37}")
        print("-" * 75)
        print(f"{'Link Calibration RPC':<28} {cal_status:<10} {results['cal_details']:<37}")
        print(f"{'PolTracking MQTT Stream':<28} {stream_status:<10} {results['stream_details']:<37}")
        print(f"{'Invalid Path Rejection':<28} {invalid_status:<10} {results['invalid_details']:<37}")
        print(f"{'Processes Alive':<28} {procs_status:<10} {procs_details:<37}")
        print(f"{'Log Check':<28} {log_status:<10} {'clean' if log_ok else 'errors found':<37}")
        print("=" * 75)

        if log_errors:
            print("\nLog errors found:")
            for agent, line in log_errors[:10]:
                print(f"  {agent}: {line}")

        print("\nLog files:")
        for agent, path in log_files:
            print(f"  {agent}: {path}")

        success = (
            results["cal_ok"]
            and results["stream_ok"]
            and results["invalid_ok"]
            and procs_ok
            and log_ok
        )
        return success

    finally:
        if server is not None:
            try:
                await server.stop()
            except Exception:
                pass
        log.info("Terminating processes...")
        terminate_all(procs)


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        log.info("Interrupted")
        sys.exit(1)
    except Exception as e:
        log.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)
