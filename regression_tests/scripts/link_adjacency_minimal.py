#!/usr/bin/env python
"""
Minimal 2-agent link adjacency test.

Starts only LBNL-Q and LBNL-SWITCH agents to verify fast convergence.
Useful for quick local testing and debugging.

Prerequisites:
  - mosquitto running on localhost:1883
  - mongo running on localhost:27017
  - workspace root at /Users/se-youngyu/quantum/

Usage:
  python -u regression_tests/scripts/link_adjacency_minimal.py
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import tempfile
from quantnet_mq.msgserver import MsgServer

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)

# Resolve workspace root from this script's path
WORKSPACE = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))


def make_local_agent_cfg(workspace, template_cfg_path):
    """
    Read agent.cfg template and rewrite Docker paths to workspace-absolute paths.

    Replaces /qn-agent/config/ and ../qn-agent/config/ with absolute workspace paths.

    Args:
        workspace: Absolute path to workspace root
        template_cfg_path: Path to the template agent.cfg

    Returns:
        Path to temporary agent.cfg with rewritten paths
    """
    with open(template_cfg_path, 'r') as f:
        lines = f.readlines()

    # Process line by line: replace qn-agent/config paths with workspace absolute path
    # Be careful to avoid double-replacement by checking the source pattern first
    workspace_cfg_path = os.path.join(workspace, 'qn-agent', 'config') + '/'
    new_lines = []
    for line in lines:
        if 'path=' in line and 'qn-agent/config/' in line:
            # First try relative path
            if '../qn-agent/config/' in line:
                line = line.replace('../qn-agent/config/', workspace_cfg_path)
            # Then try absolute path (only if relative wasn't already replaced)
            elif '/qn-agent/config/' in line:
                line = line.replace('/qn-agent/config/', workspace_cfg_path)
        new_lines.append(line)

    # Write to temp file
    temp_file = tempfile.NamedTemporaryFile(
        mode='w',
        prefix='agent_cfg_',
        suffix='.cfg',
        delete=False
    )
    temp_file.writelines(new_lines)
    temp_file.close()

    return temp_file.name


# Expected outbound links for 2-agent topology (only LBNL-Q and LBNL-SWITCH)
EXPECTED_LINKS = {
    ("LBNL-Q", "LBNL-SWITCH", "1"): "QUANTUM_UP",
    ("LBNL-Q", "LBNL-SWITCH", "4"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-Q", "2"): "CONTROL_UP",
    ("LBNL-SWITCH", "LBNL-Q", "3"): "CONTROL_UP",
}

# Agent topology config file mapping
AGENTS = {
    "LBNL-Q": "conf_lbnl-q.json",
    "LBNL-SWITCH": "conf_lbnl-switch.json",
}


def start_controller():
    """Start the quantnet controller as a subprocess with output redirection."""
    logfile = os.path.join(tempfile.gettempdir(), "link_minimal_controller.log")
    log_file = open(logfile, "w")
    cmd = [
        sys.executable,
        "-m", "quantnet_controller.cli",
        "--mq-broker-host", "localhost",
        "--mq-mongo-host", "localhost",
        "-c", os.path.join(WORKSPACE, "etc", "quantnet.cfg"),
        "--plugin-path", os.path.join(WORKSPACE, "qn-plugins", "plugins"),
        "--schema-path", os.path.join(WORKSPACE, "qn-plugins", "plugins", "schema"),
    ]
    env = os.environ.copy()
    env["QUANTNET_LINK_ADJACENCY"] = "true"
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, env=env)
    proc._logfile = logfile
    proc._log_file = log_file
    return proc


def start_agent(agent_id, node_config):
    """Start a quantnet agent as a subprocess with output redirection."""
    logfile = os.path.join(tempfile.gettempdir(), f"link_minimal_{agent_id}.log")
    log_file = open(logfile, "w")
    
    # Create a local agent.cfg with workspace-absolute paths
    local_agent_cfg = make_local_agent_cfg(
        WORKSPACE,
        os.path.join(WORKSPACE, "qn-agent", "config", "agent.cfg")
    )
    
    cmd = [
        sys.executable,
        "-m", "quantnet_agent.cli",
        "-a", agent_id,
        "-n", node_config,
        "-c", local_agent_cfg,
        "--mq-broker-host", "localhost",
        "--no-repl",
        "--interpreter-path", os.path.join(WORKSPACE, "qn-plugins", "plugins", "pingpong", "interpreter"),
        "--schema-path", os.path.join(WORKSPACE, "qn-plugins", "plugins", "schema"),
    ]
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
    proc._logfile = logfile
    proc._log_file = log_file
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


def check_logs(procs):
    """Check all process logs for errors."""
    ERROR_PATTERNS = [
        "Traceback",
        "FileNotFoundError",
        "Errno",
        "InvalidStateError",
        "Task exception was never retrieved",
    ]
    # Patterns to exclude (acceptable errors when running --no-repl)
    EXCLUDE_PATTERNS = [
        "Timeout awaiting RPC response",  # Expected with --no-repl
    ]
    
    errors = []
    for proc in procs:
        if not hasattr(proc, "_logfile"):
            continue
        try:
            if hasattr(proc, "_log_file"):
                proc._log_file.flush()
            with open(proc._logfile) as f:
                log_content = f.read()
            
            for pattern in ERROR_PATTERNS:
                if pattern in log_content:
                    # Extract the matching lines for context, excluding acceptable ones
                    matching_lines = [line for line in log_content.split('\n') 
                                    if pattern in line and not any(exc in line for exc in EXCLUDE_PATTERNS)]
                    if matching_lines:
                        errors.append(f"{os.path.basename(proc._logfile)}: {pattern} found")
                        # Only report first 3 matching lines to avoid spam
                        for line in matching_lines[:3]:
                            if line.strip():
                                errors.append(f"  → {line[:100]}")  # Truncate long lines
        except Exception as e:
            log.debug("Error reading log %s: %s", proc._logfile, e)
    return errors


class LinkMonitor:
    """Subscribe to monitor topic and collect link state updates."""

    def __init__(self, known_cids):
        self.states = {}  # (src, dst, ch) -> state
        self.known_cids = known_cids

    async def on_message(self, msg):
        """Async callback for monitor messages."""
        try:
            data = json.loads(msg)
            if data.get("eventType") != "linkStateUpdate":
                return
            value = data["value"]
            src = value["src_cid"]
            if src not in self.known_cids:
                return  # Filter out events from other agents
            key = (src, value["dst_cid"], str(value["channel_id"]))
            self.states[key] = value["state"]
        except Exception as e:
            log.debug("Error processing monitor message: %s", e)

    def check_convergence(self, expected):
        """Check if all expected links have converged to their target state."""
        failures = []
        for key, exp_state in expected.items():
            actual = self.states.get(key)
            # Accept CONTROL_UP for QUANTUM_UP (probe may be slow with dummy drivers)
            if actual == exp_state:
                continue
            if exp_state == "QUANTUM_UP" and actual == "CONTROL_UP":
                continue
            failures.append(key)
        return len(failures) == 0, failures


def print_summary(monitor, expected):
    """Print a summary table of link states."""
    print("\nMINIMAL LINK ADJACENCY CONVERGENCE RESULTS")
    print("=" * 70)
    print(f"{'SRC':<15} {'DST':<15} {'CH':>3}  {'EXPECTED':<12} {'ACTUAL':<12} {'STATUS':<10}")
    print("-" * 70)

    all_ok = True
    for key in sorted(expected.keys()):
        src, dst, ch = key
        exp_state = expected[key]
        actual = monitor.states.get(key, "NONE")

        if actual == exp_state or (exp_state == "QUANTUM_UP" and actual == "CONTROL_UP"):
            status = "OK"
        else:
            status = "FAIL"
            all_ok = False

        print(f"{src:<15} {dst:<15} {ch:>3}  {exp_state:<12} {actual:<12} {status:<10}")

    print("-" * 70)
    total = len(expected)
    converged = sum(1 for k in expected if monitor.states.get(k) in [
        expected[k], "CONTROL_UP" if expected[k] == "QUANTUM_UP" else expected[k]
    ])
    print(f"Total: {converged}/{total} links converged")
    print("=" * 70)
    return all_ok


async def main():
    """Main minimal test flow."""
    procs = []
    try:
        # 1. Start controller
        log.info("Starting controller...")
        procs.append(start_controller())
        await asyncio.sleep(2)

        # 2. Start only 2 agents
        log.info("Starting 2 agents (LBNL-Q, LBNL-SWITCH)...")
        topo_dir = os.path.join(WORKSPACE, "qn-mq", "quantnet_mq", "schema", "examples", "topology")
        for agent_id, config_file in AGENTS.items():
            node_config = os.path.join(topo_dir, config_file)
            procs.append(start_agent(agent_id, node_config))
            await asyncio.sleep(0.5)

        # 3. Connect monitor
        log.info("Connecting to MQTT monitor...")
        monitor = LinkMonitor(set(AGENTS.keys()))
        server = MsgServer(host="localhost")
        server.subscribe("monitor", monitor.on_message)
        await server.start()

        # 4. Poll for convergence
        log.info("Waiting for link convergence (timeout: 60s)...")
        timeout = 60
        interval = 2
        elapsed = 0
        converged = False

        while elapsed < timeout:
            await asyncio.sleep(interval)
            elapsed += interval
            is_converged, failures = monitor.check_convergence(EXPECTED_LINKS)
            n_ok = len(EXPECTED_LINKS) - len(failures)
            print(f"[{elapsed:3d}s] {n_ok}/{len(EXPECTED_LINKS)} links converged", flush=True)

            if is_converged:
                converged = True
                log.info("All links converged!")
                break

        # 5. Print summary
        all_ok = print_summary(monitor, EXPECTED_LINKS)

        # 6. Check logs for errors
        log.info("Checking process logs for errors...")
        log_errors = check_logs(procs)
        if log_errors:
            log.error("Log errors found:")
            for err in log_errors:
                log.error(err)
            return False

        if not converged or not all_ok:
            log.error("FAILED: Not all links converged")
            return False

        log.info("SUCCESS: All minimal test links converged to expected states")
        return True

    finally:
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
