"""
test_rgw_concentrators.py - Test RGW concentrators (HAProxy integration) for restart, stop, remove, and recovery

Usage: test_rgw_concentrators.py -c <input_yaml>

<input_yaml>
    test_rgw_concentrators.yaml

Operation:
    Verify HAProxy and RGW service status
    Test HAProxy restart and RGW access
    Test HAProxy stop and RGW inaccessibility
    Test RGW service removal and reapplication
    Test RGW daemon failure and recovery
"""

import os
import sys
import json
import time
import logging
import traceback
import subprocess
import argparse

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo
from v2.utils.utils import RGWService, connect_remote
from v2.tests.s3_swift import reusable

log = logging.getLogger()
TEST_DATA_PATH = None

class RGWConcentratorTestError(RGWBaseException):
    """Custom exception for RGW concentrator test failures"""
    pass

def run_ceph_command(ssh_con, cmd):
    """Execute a Ceph command and return JSON output."""
    try:
        if ssh_con:
            cmd = ["ssh", ssh_con.host, *cmd]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        log.info(f"Command {' '.join(cmd)} succeeded: {result.stdout}")
        return json.loads(result.stdout) if result.stdout.strip() else {}
    except subprocess.CalledProcessError as e:
        log.error(f"Command {' '.join(cmd)} failed: {e.stderr}")
        raise RGWConcentratorTestError(f"Ceph command failed: {e.stderr}")

def check_haproxy_status(ssh_con, node):
    """Check if HAProxy is running on the specified node."""
    cmd = ["systemctl", "is-active", "haproxy"]
    if ssh_con:
        cmd = ["ssh", node, *cmd]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip() == "active"
    except subprocess.CalledProcessError:
        log.warning(f"HAProxy not active on {node}")
        return False

def wait_for_daemon_running(ssh_con, daemon_type, service_name, node, timeout=60):
    """Wait for a daemon to reach running state."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        daemons = run_ceph_command(ssh_con, ["ceph", "orch", "ps", "--daemon_type", daemon_type, "--format", "json"])
        for daemon in daemons:
            if daemon.get("hostname") == node and daemon.get("daemon_id").startswith(service_name):
                if daemon.get("status_desc") == "running":
                    log.info(f"{daemon_type} daemon {daemon['daemon_id']} running on {node}")
                    return True
        log.info(f"Waiting for {daemon_type} daemon {service_name} on {node} to start...")
        time.sleep(5)
    log.error(f"{daemon_type} daemon {service_name} on {node} failed to start within {timeout}s")
    return False

def verify_rgw_access(ssh_con, node, port, expect_failure=False):
    """Verify RGW access through HAProxy."""
    cmd = ["curl", "-s", f"http://{node}:{port}"]
    if ssh_con:
        cmd = ["ssh", ssh_con.host, *cmd]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if expect_failure:
            raise RGWConcentratorTestError("RGW access succeeded unexpectedly")
        if "<ListAllMyBucketsResult" in result.stdout:
            log.info(f"RGW access verified successfully on {node}:{port}")
        else:
            raise RGWConcentratorTestError(f"Unexpected RGW response on {node}:{port}")
    except subprocess.CalledProcessError as e:
        if not expect_failure:
            raise RGWConcentratorTestError(f"RGW access failed on {node}:{port}: {e.stderr}")
        log.info(f"RGW access failed as expected on {node}:{port}")

def verify_haproxy_monitor(ssh_con, node, port, user, password):
    """Verify HAProxy monitor port access with authentication."""
    cmd = ["curl", "-s", "-u", f"{user}:{password}", f"http://{node}:{port}/stats"]
    if ssh_con:
        cmd = ["ssh", ssh_con.host, *cmd]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if "Statistics Report for HAProxy" in result.stdout:
            log.info(f"HAProxy monitor port {port} accessible on {node}")
            return True
        else:
            log.warning(f"Unexpected response from HAProxy monitor on {node}")
            return False
    except subprocess.CalledProcessError as e:
        log.error(f"Failed to access HAProxy monitor on {node}: {e.stderr}")
        return False

def test_exec(config, ssh_con):
    """Execute RGW concentrator tests."""
    rgw_service = RGWService()
    haproxy_nodes = config.haproxy_nodes
    rgw_service_name = config.rgw_service
    frontend_port = config.frontend_port
    monitor_port = config.monitor_port
    monitor_user = config.monitor_user
    monitor_password = config.monitor_password
    startup_timeout = config.startup_timeout

    # Test 1: Restart HAProxy
    log.info("Starting HAProxy restart test")
    for node in haproxy_nodes:
        log.info(f"Restarting HAProxy on {node}")
        cmd = ["systemctl", "restart", "haproxy"]
        if ssh_con:
            cmd = ["ssh", node, "sudo", *cmd]
        subprocess.run(cmd, check=True)
        time.sleep(5)
        if not check_haproxy_status(ssh_con, node):
            raise RGWConcentratorTestError(f"HAProxy failed to restart on {node}")
        if not wait_for_daemon_running(ssh_con, "haproxy", "haproxy.foo", node, startup_timeout):
            raise RGWConcentratorTestError(f"HAProxy daemon failed to stabilize on {node}")
        verify_rgw_access(ssh_con, node, frontend_port)
        verify_haproxy_monitor(ssh_con, node, monitor_port, monitor_user, monitor_password)

    # Test 2: Stop HAProxy
    log.info("Starting HAProxy stop test")
    for node in haproxy_nodes:
        log.info(f"Stopping HAProxy on {node}")
        cmd = ["systemctl", "stop", "haproxy"]
        if ssh_con:
            cmd = ["ssh", node, "sudo", *cmd]
        subprocess.run(cmd, check=True)
        time.sleep(5)
        if check_haproxy_status(ssh_con, node):
            raise RGWConcentratorTestError(f"HAProxy still running on {node}")
    verify_rgw_access(ssh_con, haproxy_nodes[0], frontend_port, expect_failure=True)
    # Recover by restarting HAProxy
    for node in haproxy_nodes:
        log.info(f"Starting HAProxy on {node}")
        cmd = ["systemctl", "start", "haproxy"]
        if ssh_con:
            cmd = ["ssh", node, "sudo", *cmd]
        subprocess.run(cmd, check=True)
        time.sleep(5)
        if not check_haproxy_status(ssh_con, node):
            raise RGWConcentratorTestError(f"HAProxy failed to start on {node}")
        if not wait_for_daemon_running(ssh_con, "haproxy", "haproxy.foo", node, startup_timeout):
            raise RGWConcentratorTestError(f"HAProxy daemon failed to stabilize on {node}")
    verify_rgw_access(ssh_con, haproxy_nodes[0], frontend_port)
    verify_haproxy_monitor(ssh_con, haproxy_nodes[0], monitor_port, monitor_user, monitor_password)

    # Test 3: Remove and reapply RGW service
    log.info("Starting RGW service remove test")
    run_ceph_command(ssh_con, ["ceph", "orch", "rm", rgw_service_name])
    time.sleep(10)
    verify_rgw_access(ssh_con, haproxy_nodes[0], frontend_port, expect_failure=True)
    # Reapply RGW service
    spec = {
        "service_type": "rgw",
        "service_id": "foo",
        "service_name": rgw_service_name,
        "placement": {
            "count_per_host": config.rgw_count_per_host,
            "nodes": haproxy_nodes
        },
        "spec": {
            "concentrator": "haproxy",
            "concentrator_frontend_port": frontend_port,
            "concentrator_monitor_port": monitor_port,
            "concentrator_monitor_user": monitor_user
        }
    }
    spec_path = os.path.join(TEST_DATA_PATH, "rgw_spec.yaml")
    with open(spec_path, "w") as f:
        json.dump(spec, f)
    run_ceph_command(ssh_con, ["ceph", "orch", "apply", "-i", spec_path])
    for node in haproxy_nodes:
        if not wait_for_daemon_running(ssh_con, "rgw", rgw_service_name, node, startup_timeout):
            raise RGWConcentratorTestError(f"RGW daemon failed to stabilize on {node}")
        if not wait_for_daemon_running(ssh_con, "haproxy", "haproxy.foo", node, startup_timeout):
            raise RGWConcentratorTestError(f"HAProxy daemon failed to stabilize on {node}")
    verify_rgw_access(ssh_con, haproxy_nodes[0], frontend_port)
    verify_haproxy_monitor(ssh_con, haproxy_nodes[0], monitor_port, monitor_user, monitor_password)

    # Test 4: RGW daemon recovery
    log.info("Starting RGW recovery test")
    for node in haproxy_nodes:
        rgw_daemons = run_ceph_command(ssh_con, ["ceph", "orch", "ps", "--daemon_type", "rgw", "--format", "json"])
        target_daemon = next((d for d in rgw_daemons if d["hostname"] == node and rgw_service_name in d["daemon_id"]), None)
        if not target_daemon:
            raise RGWConcentratorTestError(f"No RGW daemon found for {rgw_service_name} on {node}")
        daemon_id = target_daemon["daemon_id"]
        log.info(f"Stopping RGW daemon {daemon_id} on {node}")
        run_ceph_command(ssh_con, ["ceph", "orch", "daemon", "stop", daemon_id])
        time.sleep(5)
        verify_rgw_access(ssh_con, node, frontend_port)  # HAProxy should route to other daemons
        log.info(f"Restarting RGW daemon {daemon_id} on {node}")
        run_ceph_command(ssh_con, ["ceph", "orch", "daemon", "start", daemon_id])
        if not wait_for_daemon_running(ssh_con, "rgw", rgw_service_name, node, startup_timeout):
            raise RGWConcentratorTestError(f"RGW daemon {daemon_id} failed to stabilize on {node}")
        verify_rgw_access(ssh_con, node, frontend_port)
        verify_haproxy_monitor(ssh_con, node, monitor_port, monitor_user, monitor_password)

    # Check for crashes
    crash_info = reusable.check_for_crash()
    if crash_info:
        raise TestExecError("Ceph daemon crash found!")

if __name__ == "__main__":
    test_info = AddTestInfo("Test RGW Concentrators with HAProxy")
    test_info.started_info()

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info(f"TEST_DATA_PATH: {TEST_DATA_PATH}")
        if not os.path.exists(TEST_DATA_PATH):
            log.info("Test data dir not exists, creating...")
            os.makedirs(TEST_DATA_PATH)

        usage = """
        Usage:
          python3 test_rgw_concentrators.py -c test_rgw_concentrators.yaml
        """
        parser = argparse.ArgumentParser(description=usage)
        parser.add_argument("-c", dest="config", help=usage, required=True)
        parser.add_argument(
            "-log_level",
            dest="log_level",
            help="Set Log Level [DEBUG, INFO, WARNING, ERROR, CRITICAL]",
            default="info",
        )
        parser.add_argument(
            "--rgw-node", dest="rgw_node", help="RGW Node", default="127.0.0.1"
        )
        args = parser.parse_args()
        yaml_file = args.config
        rgw_node = args.rgw_node
        ssh_con = None
        if rgw_node != "127.0.0.1":
            ssh_con = connect_remote(rgw_node)
        log_f_name = os.path.basename(os.path.splitext(yaml_file)[0])
        configure_logging(f_name=log_f_name, set_level=args.log_level.upper())
        config = Config(yaml_file)
        config.read(ssh_con)

        test_exec(config, ssh_con)
        test_info.success_status("Test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("Test failed")
        sys.exit(1)
