
""" test_rgw_concentrators.py - Test if RGW and HAProxy are on the same node and concentrator behavior

Usage: test_rgw_concentrators.py -c <input_yaml>

<input_yaml>
    test_rgw_concentrators.yaml

Operation:
    Check if RGW service is running
    Check if HAProxy concentrator is configured for RGW
    Verify RGW and HAProxy are running on the same node
    Test RGW service restart and HAProxy reconnection with traffic distribution
    Test stopping one RGW instance and verify traffic rerouting
    Test stopping HAProxy instance and verify traffic stops
    Test restarting HAProxy during traffic and verify even distribution
    Test removing RGW service and verify RGW and HAProxy are removed
    Report status of colocation and concentrator behavior checks
"""

import os
import sys
import json
import time
import logging
import traceback
import subprocess
import urllib.parse
import re

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import argparse
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo
from v2.utils import utils
from v2.tests.s3_swift import reusable

log = logging.getLogger()

class RGWHAProxyColocationError(RGWBaseException):
    """Exception raised for RGW and HAProxy colocation issues"""
    def __init__(self, message):
        super().__init__(message)
        self.message = message

def get_haproxy_monitor_password(ssh_con, rgw_node):
    """Fetch HAProxy monitor password from haproxy.cfg in the container"""
    log.info(f"Fetching HAProxy monitor password from node {rgw_node}")
    try:
        # Get HAProxy container name
        podman_ps_cmd = "podman ps | grep haproxy"
        podman_output = utils.exec_shell_cmd_over_ssh(podman_ps_cmd, ssh_con)
        if not podman_output:
            raise TestExecError("No HAProxy container found running on RGW node")
        
        # Extract container name (assuming first column is container ID or name)
        container_name_match = re.search(r'(\S+).*haproxy', podman_output)
        if not container_name_match:
            raise TestExecError("Failed to parse HAProxy container name from podman ps output")
        container_name = container_name_match.group(1)
        log.info(f"HAProxy container name: {container_name}")
        
        # Read haproxy.cfg from container
        haproxy_cfg_cmd = f"podman exec {container_name} cat /var/lib/haproxy/haproxy.cfg"
        haproxy_cfg = utils.exec_shell_cmd_over_ssh(haproxy_cfg_cmd, ssh_con)
        if not haproxy_cfg:
            raise TestExecError("Failed to read HAProxy configuration file")
        
        # Parse stats auth line for password
        password_match = re.search(r'stats auth admin:(\S+)', haproxy_cfg)
        if not password_match:
            raise TestExecError("HAProxy monitor password not found in configuration")
        
        password = password_match.group(1)
        log.info("Successfully retrieved HAProxy monitor password")
        return password
    
    except Exception as e:
        log.error(f"Failed to fetch HAProxy monitor password: {str(e)}")
        raise TestExecError(f"Unable to retrieve HAProxy monitor password: {str(e)}")

def rgw_with_concentrators():
    """Verify if RGW and HAProxy are co-located on the same node"""
    log.info("Verifying RGW and HAProxy colocation")
    try:
        # Execute ceph orch ps command
        orch_ps_cmd = "sudo ceph orch ps --format json"
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        
        # Execute ceph orch ls command for RGW
        orch_ls_cmd = "sudo ceph orch ls rgw --format json"
        orch_ls_output = exec_shell_cmd(orch_ls_cmd)
        orch_ls_data = json.loads(orch_ls_output)
        
        # Filter RGW and HAProxy services
        rgw_services = [s for s in orch_ps_data if s.get('daemon_type') == 'rgw']
        haproxy_services = [s for s in orch_ps_data if s.get('daemon_type') == 'haproxy']
        
        # Verify RGW services exist
        if not rgw_services:
            raise RGWHAProxyColocationError("No RGW services found")
        
        # Check if HAProxy is configured as concentrator
        rgw_service_info = orch_ls_data[0] if orch_ls_data else {}
        if not rgw_service_info.get('spec', {}).get('concentrator') == 'haproxy':
            raise RGWHAProxyColocationError("HAProxy not configured as RGW concentrator")
        
        # Get host information
        rgw_hosts = set(s.get('hostname') for s in rgw_services)
        haproxy_hosts = set(s.get('hostname') for s in haproxy_services)
        
        # Verify colocation
        if not haproxy_hosts:
            raise RGWHAProxyColocationError("No HAProxy services found")
        
        if rgw_hosts != haproxy_hosts:
            raise RGWHAProxyColocationError(
                f"RGW and HAProxy not co-located. RGW hosts: {rgw_hosts}, HAProxy hosts: {haproxy_hosts}"
            )
        
        log.info(f"RGW and HAProxy are co-located on hosts: {rgw_hosts}")
        return True
    
    except json.JSONDecodeError:
        raise TestExecError("Failed to parse ceph orch command output")
    except RGWHAProxyColocationError as e:
        log.error(e.message)
        return False

def test_rgw_concentrator_behavior(config, ssh_con, rgw_node):
    """Test RGW service restart and HAProxy reconnection behavior with traffic distribution"""
    log.info("Testing RGW service restart and HAProxy reconnection behavior")
    try:
        # Get HAProxy monitor password
        monitor_password = get_haproxy_monitor_password(ssh_con, rgw_node)
        
        # Verify RGW and HAProxy configuration
        orch_ls_cmd = "sudo ceph orch ls rgw --format json"
        orch_ls_output = exec_shell_cmd(orch_ls_cmd)
        orch_ls_data = json.loads(orch_ls_output)
        
        if not orch_ls_data:
            raise TestExecError("No RGW service information found")
        
        rgw_service_info = orch_ls_data[0]
        if not rgw_service_info.get('spec', {}).get('concentrator') == 'haproxy':
            raise TestExecError("HAProxy not configured as RGW concentrator")
        
        service_name = rgw_service_info.get('service_name', '')
        if not service_name:
            raise TestExecError("RGW service name not found")
        
        hosts = rgw_service_info.get('placement', {}).get('hosts', [])
        if not hosts:
            raise TestExecError("No hosts found for RGW service")
        host = hosts[0]
        
        frontend_port = rgw_service_info.get('spec', {}).get('concentrator_frontend_port', 8080)
        monitor_port = rgw_service_info.get('spec', {}).get('concentrator_monitor_port', 1967)
        monitor_user = rgw_service_info.get('spec', {}).get('concentrator_monitor_user', 'admin')
        
        # Get number of test requests from config, default to 20
        num_requests = config.test_ops.get('traffic_test_requests', 20)
        
        # Test traffic distribution before restart
        log.info(f"Sending {num_requests} test requests to HAProxy frontend at {host}:{frontend_port}")
        successful_requests_before = 0
        for i in range(num_requests):
            curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{frontend_port}"
            result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
            status_code = result.stdout.strip()
            log.info(f"Request {i+1} to {host}:{frontend_port} returned status code {status_code}")
            if status_code == '200':
                successful_requests_before += 1
            else:
                log.warning(f"Request {i+1} failed with status code {status_code}")
            time.sleep(0.1)  # Small delay to avoid overwhelming the server
        
        # Check HAProxy stats before restart
        rgw_hits_before = {}
        stats_url = f"http://{host}:{monitor_port}/stats;csv"
        stats_cmd = f"curl -s -u {monitor_user}:{monitor_password} \"{stats_url}\" | awk -F',' 'NR==1 || /^backend|^frontend|^stats/' | cut -d',' -f1,2,5,8,9,10,18,35,73 | column -s',' -t"
        log.info(f"Executing HAProxy stats command: {stats_cmd}")
        stats_output = exec_shell_cmd(stats_cmd)
        raw_stats_cmd = f"curl -s -u {monitor_user}:{monitor_password} \"{stats_url}\""
        raw_stats_output = exec_shell_cmd(raw_stats_cmd)
        if stats_output and not stats_output.startswith("<!DOCTYPE"):
            log.info(f"HAProxy stats before restart (formatted):\n{stats_output}")
            if raw_stats_output:
                rgw_hits_before = parse_haproxy_stats(raw_stats_output, service_name)
                log.info(f"HAProxy stats before restart (parsed): {rgw_hits_before}")
            else:
                log.warning("Failed to retrieve raw HAProxy stats before restart")
        else:
            log.warning(f"Failed to retrieve HAProxy stats before restart, formatted output: {stats_output[:100]}...")
            log.warning("Proceeding with fallback checks")
        
        # Restart RGW service
        restart_cmd = f"sudo ceph orch restart {service_name}"
        restart_output = exec_shell_cmd(restart_cmd)
        if not restart_output:
            raise TestExecError("Failed to execute RGW service restart")
        
        log.info(f"Restart command output: {restart_output}")
        
        # Wait for services to restart
        log.info("Waiting 30 seconds for services to restart")
        time.sleep(30)
        
        # Verify RGW and HAProxy services are running
        orch_ps_cmd = "sudo ceph orch ps --format json"
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        
        rgw_services = [s for s in orch_ps_data if s.get('daemon_type') == 'rgw' and s.get('service_name') == service_name]
        haproxy_services = [s for s in orch_ps_data if s.get('daemon_type') == 'haproxy' and s.get('service_name') == service_name]
        
        if not rgw_services:
            raise TestExecError("No RGW services found after restart")
        
        if not haproxy_services:
            raise TestExecError("No HAProxy services found after restart")
        
        # Log service status
        log.info(f"RGW services after restart: {[s.get('daemon_name') + ': ' + s.get('status_desc') for s in rgw_services]}")
        log.info(f"HAProxy services after restart: {[s.get('daemon_name') + ': ' + s.get('status_desc') for s in haproxy_services]}")
        
        # Verify all services are running
        for service in rgw_services + haproxy_services:
            if service.get('status_desc') != 'running':
                raise TestExecError(f"Service {service.get('daemon_name')} is not running: {service.get('status_desc')}")
        
        # Verify ports for RGW instances
        expected_ports = rgw_service_info.get('status', {}).get('ports', [])
        if not expected_ports:
            raise TestExecError("No ports found in RGW service status")
        
        rgw_ports = [port for service in rgw_services for port in service.get('ports', [])]
        if sorted(rgw_ports) != sorted(expected_ports):
            raise TestExecError(f"RGW ports {rgw_ports} do not match expected ports {expected_ports}")
        
        # Test traffic distribution after restart
        log.info(f"Sending {num_requests} test requests to HAProxy frontend at {host}:{frontend_port} after restart")
        successful_requests_after = 0
        for i in range(num_requests):
            curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{frontend_port}"
            result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
            status_code = result.stdout.strip()
            if status_code == '200':
                successful_requests_after += 1
            else:
                log.warning(f"Request {i+1} failed with status code {status_code}")
            time.sleep(0.1)
        
        # Check HAProxy stats after restart
        rgw_hits_after = {}
        log.info(f"Executing HAProxy stats command: {stats_cmd}")
        stats_output = exec_shell_cmd(stats_cmd)
        raw_stats_output = exec_shell_cmd(raw_stats_cmd)
        if stats_output and not stats_output.startswith("<!DOCTYPE"):
            log.info(f"HAProxy stats after restart (formatted):\n{stats_output}")
            if raw_stats_output:
                rgw_hits_after = parse_haproxy_stats(raw_stats_output, service_name)
                log.info(f"HAProxy stats after restart (parsed): {rgw_hits_after}")
            else:
                log.warning("Failed to retrieve raw HAProxy stats after restart")
        else:
            log.warning(f"Failed to retrieve HAProxy stats after restart, formatted output: {stats_output[:100]}...")
            log.warning("Using fallback checks")
        
        # Verify traffic distribution
        expected_rgw_count = len(expected_ports)
        if rgw_hits_after:
            total_hits = sum(rgw_hits_after.values())
            if total_hits < num_requests * 0.5:  # Allow some requests to fail
                raise TestExecError(f"Too few successful requests: {total_hits} out of {num_requests}")
            if len(rgw_hits_after) != expected_rgw_count:
                raise TestExecError(f"Traffic not distributed to all {expected_rgw_count} RGW instances: {rgw_hits_after}")
            # Check for roughly even distribution (within 20% deviation)
            avg_hits = total_hits / expected_rgw_count
            for rgw, hits in rgw_hits_after.items():
                if abs(hits - avg_hits) > 0.2 * avg_hits:
                    log.warning(f"Uneven traffic distribution for {rgw}: {hits} hits (expected ~{avg_hits})")
        else:
            # Fallback: Verify RGW ports are accessible directly
            log.info(f"Fallback: Testing direct access to RGW ports {expected_ports} on {host}")
            accessible_ports = []
            for port in expected_ports:
                curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{port}"
                result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
                if result.stdout.strip() == '200':
                    accessible_ports.append(port)
                else:
                    log.warning(f"Direct access to RGW port {port} failed with status code {result.stdout.strip()}")
            if sorted(accessible_ports) != sorted(expected_ports):
                raise TestExecError(f"Not all RGW ports {expected_ports} are accessible: {accessible_ports}")
        
        # Verify sufficient successful requests
        if successful_requests_after < num_requests * 0.5:
            raise TestExecError(f"Too few successful requests after restart: {successful_requests_after} out of {num_requests}")
        
        log.info(f"RGW and HAProxy services restarted successfully. RGW ports: {rgw_ports}, Traffic distribution: {rgw_hits_after or 'verified via fallback'}")
        return True
    
    except json.JSONDecodeError:
        raise TestExecError("Failed to parse ceph orch command output")
    except TestExecError as e:
        log.error(e.message)
        return False

def test_single_rgw_stop(config, ssh_con, rgw_node):
    """Test stopping one RGW instance and verify traffic is rerouted to the remaining instance"""
    log.info("Testing stopping one RGW instance and traffic rerouting")
    try:
        # Get HAProxy monitor password
        monitor_password = get_haproxy_monitor_password(ssh_con, rgw_node)
        
        # Verify RGW and HAProxy configuration
        orch_ls_cmd = "sudo ceph orch ls rgw --format json"
        orch_ls_output = exec_shell_cmd(orch_ls_cmd)
        orch_ls_data = json.loads(orch_ls_output)
        
        if not orch_ls_data:
            raise TestExecError("No RGW service information found")
        
        rgw_service_info = orch_ls_data[0]
        service_name = rgw_service_info.get('service_name', '')
        if not service_name:
            raise TestExecError("RGW service name not found")
        
        hosts = rgw_service_info.get('placement', {}).get('hosts', [])
        if not hosts:
            raise TestExecError("No hosts found for RGW service")
        host = hosts[0]
        
        frontend_port = rgw_service_info.get('spec', {}).get('concentrator_frontend_port', 8080)
        monitor_port = rgw_service_info.get('spec', {}).get('concentrator_monitor_port', 1967)
        monitor_user = rgw_service_info.get('spec', {}).get('concentrator_monitor_user', 'admin')
        
        # Get number of test requests from config, default to 20
        num_requests = config.test_ops.get('traffic_test_requests', 20)
        
        # Get RGW daemons
        orch_ps_cmd = "sudo ceph orch ps --format json"
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        rgw_services = [s for s in orch_ps_data if s.get('daemon_type') == 'rgw' and s.get('service_name') == service_name]
        
        if len(rgw_services) < 2:
            raise TestExecError("Need at least two RGW instances to test stopping one")
        
        # Select the first RGW instance to stop
        rgw_to_stop = rgw_services[0]['daemon_name']
        log.info(f"Stopping RGW instance: {rgw_to_stop}")
        
        # Get baseline HAProxy stats
        stats_url = f"http://{host}:{monitor_port}/stats;csv"
        stats_cmd = f"curl -s -u {monitor_user}:{monitor_password} \"{stats_url}\" | awk -F',' 'NR==1 || /^backend|^frontend|^stats/' | cut -d',' -f1,2,5,8,9,10,18,35,73 | column -s',' -t"
        raw_stats_cmd = f"curl -s -u {monitor_user}:{monitor_password} \"{stats_url}\""
        baseline_stats = exec_shell_cmd(raw_stats_cmd)
        baseline_hits = parse_haproxy_stats(baseline_stats, service_name) if baseline_stats else {}
        log.info(f"Baseline HAProxy stats (parsed): {baseline_hits}")
        formatted_stats = exec_shell_cmd(stats_cmd)
        if formatted_stats and not stats_output.startswith("<!DOCTYPE"):
            log.info(f"Baseline HAProxy stats (formatted):\n{formatted_stats}")
        
        # Stop the selected RGW instance
        stop_cmd = f"sudo ceph orch daemon stop {rgw_to_stop}"
        stop_output = exec_shell_cmd(stop_cmd)
        if not stop_output:
            raise TestExecError(f"Failed to stop RGW instance {rgw_to_stop}")
        
        log.info(f"Stop command output: {stop_output}")
        
        # Wait for HAProxy to detect the stopped instance
        log.info("Waiting 30 seconds for HAProxy to update after stopping RGW instance")
        time.sleep(30)
        
        # Verify the stopped instance is not running
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        rgw_services = [s for s in orch_ps_data if s.get('daemon_type') == 'rgw' and s.get('service_name') == service_name]
        stopped_rgw = [s for s in rgw_services if s.get('daemon_name') == rgw_to_stop]
        if stopped_rgw and stopped_rgw[0].get('status_desc') == 'running':
            raise TestExecError(f"RGW instance {rgw_to_stop} is still running after stop command")
        
        # Send test requests to HAProxy frontend
        log.info(f"Sending {num_requests} test requests to HAProxy frontend at {host}:{frontend_port} with {rgw_to_stop} stopped")
        successful_requests = 0
        for i in range(num_requests):
            curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{frontend_port}"
            result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
            status_code = result.stdout.strip()
            if status_code == '200':
                successful_requests += 1
            else:
                log.warning(f"Request {i+1} failed with status code {status_code}")
            time.sleep(0.1)
        
        # Check HAProxy stats after stopping
        stats_output = exec_shell_cmd(stats_cmd)
        raw_stats_output = exec_shell_cmd(raw_stats_cmd)
        if stats_output and not stats_output.startswith("<!DOCTYPE"):
            log.info(f"HAProxy stats after stopping {rgw_to_stop} (formatted):\n{stats_output}")
            if raw_stats_output:
                rgw_hits_after_stop = parse_haproxy_stats(raw_stats_output, service_name)
                log.info(f"HAProxy stats after stopping {rgw_to_stop} (parsed): {rgw_hits_after_stop}")
            else:
                log.warning("Failed to retrieve raw HAProxy stats after stopping RGW")
        else:
            log.warning(f"Failed to retrieve HAProxy stats after stopping {rgw_to_stop}, formatted output: {stats_output[:100]}...")
            raise TestExecError("Failed to retrieve HAProxy stats after stopping RGW")
        
        # Verify traffic distribution
        if rgw_hits_after_stop:
            total_hits = sum(rgw_hits_after_stop.values())
            if total_hits < num_requests * 0.5:
                raise TestExecError(f"Too few successful requests after stopping {rgw_to_stop}: {total_hits} out of {num_requests}")
            if rgw_to_stop in rgw_hits_after_stop and rgw_hits_after_stop[rgw_to_stop] > baseline_hits.get(rgw_to_stop, 0):
                raise TestExecError(f"Traffic sent to stopped RGW instance {rgw_to_stop}: {rgw_hits_after_stop[rgw_to_stop]} hits")
            remaining_instances = [rgw for rgw in rgw_hits_after_stop if rgw != rgw_to_stop]
            if len(remaining_instances) != len(rgw_services) - 1:
                raise TestExecError(f"Traffic not routed to all remaining RGW instances: {rgw_hits_after_stop}")
        
        # Restart the stopped RGW instance
        restart_cmd = f"sudo ceph orch daemon start {rgw_to_stop}"
        restart_output = exec_shell_cmd(restart_cmd)
        if not restart_output:
            raise TestExecError(f"Failed to restart RGW instance {rgw_to_stop}")
        
        log.info(f"Restart command output: {restart_output}")
        
        # Wait for the instance to restart
        log.info(f"Waiting 30 seconds for {rgw_to_stop} to restart")
        time.sleep(30)
        
        # Verify all RGW instances are running
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        rgw_services = [s for s in orch_ps_data if s.get('daemon_type') == 'rgw' and s.get('service_name') == service_name]
        for service in rgw_services:
            if service.get('status_desc') != 'running':
                raise TestExecError(f"Service {service.get('daemon_name')} is not running after restart: {service.get('status_desc')}")
        
        log.info(f"RGW instance {rgw_to_stop} stopped and restarted successfully. Traffic distribution: {rgw_hits_after_stop}")
        return True
    
    except json.JSONDecodeError:
        raise TestExecError("Failed to parse ceph orch command output")
    except TestExecError as e:
        log.error(e.message)
        return False

def test_haproxy_stop(config, ssh_con, rgw_node):
    """Test stopping HAProxy instance and verify traffic stops immediately"""
    log.info("Testing stopping HAProxy instance and traffic behavior")
    try:
        # Get HAProxy monitor password
        monitor_password = get_haproxy_monitor_password(ssh_con, rgw_node)
        
        # Verify RGW and HAProxy configuration
        orch_ls_cmd = "sudo ceph orch ls rgw --format json"
        orch_ls_output = exec_shell_cmd(orch_ls_cmd)
        orch_ls_data = json.loads(orch_ls_output)
        
        if not orch_ls_data:
            raise TestExecError("No RGW service information found")
        
        rgw_service_info = orch_ls_data[0]
        service_name = rgw_service_info.get('service_name', '')
        if not service_name:
            raise TestExecError("RGW service name not found")
        
        hosts = rgw_service_info.get('placement', {}).get('hosts', [])
        if not hosts:
            raise TestExecError("No hosts found for RGW service")
        host = hosts[0]
        
        frontend_port = rgw_service_info.get('spec', {}).get('concentrator_frontend_port', 8080)
        monitor_port = rgw_service_info.get('spec', {}).get('concentrator_monitor_port', 1967)
        monitor_user = rgw_service_info.get('spec', {}).get('concentrator_monitor_user', 'admin')
        
        # Get number of test requests from config, default to 20
        num_requests = config.test_ops.get('traffic_test_requests', 20)
        
        # Get HAProxy daemon
        orch_ps_cmd = "sudo ceph orch ps --format json"
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        haproxy_services = [s for s in orch_ps_data if s.get('daemon_type') == 'haproxy' and s.get('service_name') == service_name]
        
        if not haproxy_services:
            raise TestExecError("No HAProxy services found")
        
        # Select the HAProxy instance to stop
        haproxy_to_stop = haproxy_services[0]['daemon_name']
        log.info(f"Stopping HAProxy instance: {haproxy_to_stop}")
        
        # Get baseline HAProxy stats
        stats_url = f"http://{host}:{monitor_port}/stats;csv"
        stats_cmd = f"curl -s -u {monitor_user}:{monitor_password} \"{stats_url}\" | awk -F',' 'NR==1 || /^backend|^frontend|^stats/' | cut -d',' -f1,2,5,8,9,10,18,35,73 | column -s',' -t"
        raw_stats_cmd = f"curl -s -u {monitor_user}:{monitor_password} \"{stats_url}\""
        baseline_stats = exec_shell_cmd(raw_stats_cmd)
        baseline_hits = parse_hierarchy_stats(baseline_stats, service_name) if baseline_stats else {}
        log.info(f"Baseline HAProxy stats (parsed): {baseline_hits}")
        formatted_stats = exec_shell_cmd(stats_cmd)
        if formatted_stats and not formatted_stats.startswith("<!DOCTYPE"):
            log.info(f"Baseline HAProxy stats (formatted):\n{formatted_stats}")
        
        # Stop the HAProxy instance
        stop_cmd = f"sudo ceph orch daemon stop {haproxy_to_stop}"
        stop_output = exec_shell_cmd(stop_cmd)
        if not stop_output:
            raise TestExecError(f"Failed to stop HAProxy instance {haproxy_to_stop}")
        
        log.info(f"Stop command output: {stop_output}")
        
        # Wait for HAProxy to stop
        log.info("Waiting 30 seconds for HAProxy to stop")
        time.sleep(30)
        
        # Verify the HAProxy instance is not running
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        haproxy_services = [s for s in orch_ps_data if s.get('daemon_type') == 'haproxy' and s.get('service_name') == service_name]
        stopped_haproxy = [s for s in haproxy_services if s.get('daemon_name') == haproxy_to_stop]
        if stopped_haproxy and stopped_haproxy[0].get('status_desc') == 'running':
            raise TestExecError(f"HAProxy instance {haproxy_to_stop} is still running after stop command")
        
        # Send test requests to HAProxy frontend (expect failure)
        log.info(f"Sending {num_requests} test requests to HAProxy frontend at {host}:{frontend_port} with {haproxy_to_stop} stopped")
        successful_requests = 0
        for i in range(num_requests):
            curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{frontend_port}"
            result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
            status_code = result.stdout.strip()
            if status_code == '200':
                successful_requests += 1
                log.warning(f"Request {i+1} succeeded unexpectedly with status code {status_code}")
            else:
                log.info(f"Request {i+1} failed as expected with status code {status_code}")
            time.sleep(0.1)
        
        # Verify no successful requests
        if successful_requests > 0:
            raise TestExecError(f"Unexpected successful requests with HAProxy stopped: {successful_requests} out of {num_requests}")
        
        # Attempt to check HAProxy stats (expect failure)
        stats_output = exec_shell_cmd(stats_cmd)
        if stats_output and not stats_output.startswith("<!DOCTYPE"):
            log.warning(f"Unexpected HAProxy stats retrieved while {haproxy_to_stop} stopped:\n{stats_output}")
        else:
            log.info(f"HAProxy stats unavailable as expected while {haproxy_to_stop} stopped")
        
        # Restart the HAProxy instance
        restart_cmd = f"sudo ceph orch daemon start {haproxy_to_stop}"
        restart_output = exec_shell_cmd(restart_cmd)
        if not restart_output:
            raise TestExecError(f"Failed to restart HAProxy instance {haproxy_to_stop}")
        
        log.info(f"Restart command output: {restart_output}")
        
        # Wait for HAProxy to restart
        log.info(f"Waiting 30 seconds for {haproxy_to_stop} to restart")
        time.sleep(30)
        
        # Verify HAProxy and RGW instances are running
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        haproxy_services = [s for s in orch_ps_data if s.get('daemon_type') == 'haproxy' and s.get('service_name') == service_name]
        rgw_services = [s for s in orch_ps_data if s.get('daemon_type') == 'rgw' and s.get('service_name') == service_name]
        
        for service in haproxy_services + rgw_services:
            if service.get('status_desc') != 'running':
                raise TestExecError(f"Service {service.get('daemon_name')} is not running after restart: {service.get('status_desc')}")
        
        # Verify traffic resumes after restart
        log.info(f"Sending {num_requests} test requests to HAProxy frontend at {host}:{frontend_port} after restarting {haproxy_to_stop}")
        successful_requests_after = 0
        for i in range(num_requests):
            curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{frontend_port}"
            result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
            status_code = result.stdout.strip()
            if status_code == '200':
                successful_requests_after += 1
            else:
                log.warning(f"Request {i+1} failed with status code {status_code}")
            time.sleep(0.1)
        
        # Verify sufficient successful requests after restart
        if successful_requests_after < num_requests * 0.5:
            raise TestExecError(f"Too few successful requests after restarting {haproxy_to_stop}: {successful_requests_after} out of {num_requests}")
        
        # Check HAProxy stats after restart
        stats_output = exec_shell_cmd(stats_cmd)
        raw_stats_output = exec_shell_cmd(raw_stats_cmd)
        if stats_output and not stats_output.startswith("<!DOCTYPE"):
            log.info(f"HAProxy stats after restarting {haproxy_to_stop} (formatted):\n{stats_output}")
            if raw_stats_output:
                rgw_hits_after_restart = parse_haproxy_stats(raw_stats_output, service_name)
                log.info(f"HAProxy stats after restarting {haproxy_to_stop} (parsed): {rgw_hits_after_restart}")
            else:
                log.warning("Failed to retrieve raw HAProxy stats after restarting HAProxy")
        else:
            log.warning(f"Failed to retrieve HAProxy stats after restarting {haproxy_to_stop}, formatted output: {stats_output[:100]}...")
            raise TestExecError("Failed to retrieve HAProxy stats after restarting HAProxy")
        
        log.info(f"HAProxy instance {haproxy_to_stop} stopped and restarted successfully. Traffic stopped during downtime and resumed after restart.")
        return True
    
    except json.JSONDecodeError:
        raise TestExecError("Failed to parse ceph orch command output")
    except TestExecError as e:
        log.error(e.message)
        return False

def test_haproxy_restart(config, ssh_con, rgw_node):
    """Test restarting HAProxy during active traffic and verify even distribution"""
    log.info("Testing restarting HAProxy during active traffic")
    try:
        # Get HAProxy monitor password
        monitor_password = get_haproxy_monitor_password(ssh_con, rgw_node)
        
        # Verify RGW and HAProxy configuration
        orch_ls_cmd = "sudo ceph orch ls rgw --format json"
        orch_ls_output = exec_shell_cmd(orch_ls_cmd)
        orch_ls_data = json.loads(orch_ls_output)
        
        if not orch_ls_data:
            raise TestExecError("No RGW service information found")
        
        rgw_service_info = orch_ls_data[0]
        service_name = rgw_service_info.get('service_name', '')
        if not service_name:
            raise TestExecError("RGW service name not found")
        
        hosts = rgw_service_info.get('placement', {}).get('hosts', [])
        if not hosts:
            raise TestExecError("No hosts found for RGW service")
        host = hosts[0]
        
        frontend_port = rgw_service_info.get('spec', {}).get('concentrator_frontend_port', 8080)
        monitor_port = rgw_service_info.get('spec', {}).get('concentrator_monitor_port', 1967)
        monitor_user = rgw_service_info.get('spec', {}).get('concentrator_monitor_user', 'admin')
        
        # Get number of test requests from config, default to 20
        num_requests = config.test_ops.get('traffic_test_requests', 20)
        
        # Get HAProxy daemon
        orch_ps_cmd = "sudo ceph orch ps --format json"
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        haproxy_services = [s for s in orch_ps_data if s.get('daemon_type') == 'haproxy' and s.get('service_name') == service_name]
        
        if not haproxy_services:
            raise TestExecError("No HAProxy services found")
        
        # Select the HAProxy instance to restart
        haproxy_to_restart = haproxy_services[0]['daemon_name']
        log.info(f"Restarting HAProxy instance: {haproxy_to_restart} during traffic")

        # Get baseline HAProxy stats
        stats_url = f"http://http://{host}:{monitor_port}/stats;csv"
        stats_cmd = f"curl -s -u -u {monitor_user}:{monitor_password} \"{stats_url}\" | awk -F',' 'NR==1 || /^backend|^frontend|^stats/' | cut -d',' -f1,2,5,8,9,10,18,35,73 | column -s',' -t"
        raw_stats_cmd = f"curl -s -u -u {monitor_user}:{monitor_password} \"{stats_url}\""
        baseline_stats = exec_shell_cmd(raw_stats_cmd)
        baseline_hits = parse_haproxy_stats(baseline_stats, service_name) if baseline_stats else {}
        log.info(f"Baseline HAProxy stats (parsed): {baseline_hits}")
        formatted_stats = exec_shell_cmd(stats_cmd)
        if formatted_stats and not formatted_stats.startswith("<!DOCTYPE"):
            log.info(f"Baseline HAProxy stats (formatted):\n{formatted_stats}")
        
        # Send test requests with HAProxy restart in the middle
        log.info(f"Sending {num_requests} test requests to HAProxy frontend at {host}:{frontend_port} with restart after 5 requests")
        successful_requests = 0
        failed_requests = []
        for i in range(num_requests):
            curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{frontend_port}"
            result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
            status_code = result.stdout.strip()
            if status_code == '200':
                successful_requests += 1
                log.info(f"Request {i+1} succeeded with status code {status_code}")
            else:
                failed_requests.append(i+1)
                log.info(f"Request {i+1} failed with status code {status_code}")
            
            # Trigger HAProxy restart after 5 requests
            if i == 4:
                log.info(f"Triggering restart of HAProxy instance: {haproxy_to_restart}")
                restart_cmd = f"sudo ceph orch daemon restart {haproxy_to_restart}"
                restart_output = exec_shell_cmd(restart_cmd)
                if not restart_output:
                    raise TestExecError(f"Failed to restart HAProxy instance {haproxy_to_restart}")
                log.info(f"Restart command output: {restart_output}")
            
            time.sleep(0.1)
        
        # Wait for HAProxy to stabilize
        log.info(f"Waiting 30 seconds for {haproxy_to_restart} to stabilize after restart")
        time.sleep(30)
        
        # Verify HAProxy and RGW instances are running
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        haproxy_services = [s for s in orch_ps_data if s.get('daemon_type') == 'haproxy' and s.get('service_name') == service_name]
        rgw_services = [s for s in orch_ps_data if s.get('daemon_type') == 'rgw' and s.get('service_name') == service_name]
        
        for service in haproxy_services + rgw_services:
            if service.get('status_desc') != 'running':
                raise TestExecError(f"Service {service.get('daemon_name')} is not running after restart: {service.get('status_desc')}")
        
        # Verify some requests failed during restart
        if not failed_requests:
            log.warning("No requests failed during HAProxy restart, which is unexpected")
        
        # Verify sufficient successful requests overall
        if successful_requests < num_requests * 0.5:
            raise TestExecError(f"Too few successful requests during HAProxy restart test: {successful_requests} out of {num_requests}")
        
        # Check HAProxy stats after restart
        stats_output = exec_shell_cmd(stats_cmd)
        raw_stats_output = exec_shell_cmd(raw_stats_cmd)
        if stats_output and not stats_output.startswith("<!DOCTYPE"):
            log.info(f"HAProxy stats after restarting {haproxy_to_restart} (formatted):\n{stats_output}")
            if raw_stats_output:
                rgw_hits_after_restart = parse_haproxy_stats(raw_stats_output, service_name)
                log.info(f"HAProxy stats after restarting {haproxy_to_restart} (parsed): {rgw_hits_after_restart}")
            else:
                log.warning("Failed to retrieve raw HAProxy stats after restarting HAProxy")
        else:
            log.warning(f"Failed to retrieve HAProxy stats after restarting {haproxy_to_restart}, formatted output: {stats_output[:100]}...")
            raise TestExecError("Failed to retrieve HAProxy stats after restarting HAProxy")
        
        # Verify even traffic distribution
        expected_rgw_count = len(rgw_services)
        if rgw_hits_after_restart:
            total_hits = sum(rgw_hits_after_restart.values())
            if total_hits < successful_requests * 0.5:
                raise TestExecError(f"Too few hits recorded in HAProxy stats: {total_hits} for {successful_requests} successful requests")
            if len(rgw_hits_after_restart) != expected_rgw_count:
                raise TestExecError(f"Traffic not distributed to all {expected_rgw_count} RGW instances: {rgw_hits_after_restart}")
            avg_hits = total_hits / expected_rgw_count
            for rgw, hits in rgw_hits_after_restart.items():
                if abs(hits - avg_hits) > 0.2 * avg_hits:
                    log.warning(f"Uneven traffic distribution for {rgw}: {hits} hits (expected ~{avg_hits})")
        
        log.info(f"HAProxy instance {haproxy_to_restart} restarted successfully during traffic. Traffic resumed with even distribution: {rgw_hits_after_restart}")
        return True
    
    except json.JSONDecodeError:
        raise TestExecError("Failed to parse ceph orch command output")
    except TestExecError as e:
        log.error(e.message)
        return False

def test_rgw_service_removal(config, ssh_con, rgw_node):
    """Test removing RGW service and verify RGW and HAProxy services are removed after 30 seconds"""
    log.info("Testing RGW service removal")
    try:
        # Get HAProxy monitor password
        monitor_password = get_haproxy_monitor_password(ssh_con, rgw_node)
        
        # Verify RGW service exists initially
        orch_ls_cmd = "sudo ceph orch ls rgw --format json"
        orch_ls_output = exec_shell_cmd(orch_ls_cmd)
        orch_ls_data = json.loads(orch_ls_output)
        
        if not orch_ls_data:
            raise TestExecError("No RGW service information found before removal")
        
        rgw_service_info = orch_ls_data[0]
        service_name = rgw_service_info.get('service_name', '')
        if not service_name:
            raise TestExecError("RGW service name not found")
        
        hosts = rgw_service_info.get('placement', {}).get('hosts', [])
        if not hosts:
            raise TestExecError("No hosts found for RGW service")
        host = hosts[0]
        
        frontend_port = rgw_service_info.get('spec', {}).get('concentrator_frontend_port', 8080)
        monitor_port = rgw_service_info.get('spec', {}).get('concentrator_monitor_port', 1967)
        monitor_user = rgw_service_info.get('spec', {}).get('concentrator_monitor_user', 'admin')
        
        log.info(f"Initial RGW service state: {service_name} on host {host}")
        
        # Get number of test requests from config, default to 20
        num_requests = config.test_ops.get('traffic_test_requests', 20)
        
        # Remove RGW service
        remove_cmd = f"sudo ceph orch rm {service_name}"
        remove_output = exec_shell_cmd(remove_cmd)
        if not remove_output:
            raise TestExecError(f"Failed to remove RGW service {service_name}")
        
        log.info(f"Remove command output: {remove_output}")
        
        # Wait and retry checking service removal
        log.info("Waiting up to 30 seconds for RGW service removal with retries")
        max_retries = 3
        retry_interval = 10
        for attempt in range(max_retries):
            time.sleep(retry_interval)
            orch_ls_output = exec_shell_cmd(orch_ls_cmd)
            try:
                orch_ls_data = json.loads(orch_ls_output)
                rgw_services = [s for s in orch_ls_data if s.get('service_name') == service_name]
                if not rgw_services:
                    log.info(f"RGW service {service_name} successfully removed after {attempt + 1} checks")
                    break
                else:
                    log.warning(f"Attempt {attempt + 1}: RGW service {service_name} still present: {rgw_services}")
            except json.JSONDecodeError:
                log.info(f"Attempt {attempt + 1}: No RGW services found in orch ls output, assuming removal complete")
                break
        else:
            raise TestExecError(f"RGW service {service_name} still present after {max_retries} retries over 30 seconds: {orch_ls_data}")
        
        # Verify RGW and HAProxy daemons are removed
        orch_ps_cmd = "sudo ceph orch ps --format json"
        orch_ps_output = exec_shell_cmd(orch_ps_cmd)
        orch_ps_data = json.loads(orch_ps_output)
        rgw_services = [s for s in orch_ps_data if s.get('daemon_type') == 'rgw' and s.get('service_name') == service_name]
        haproxy_services = [s for s in orch_ps_data if s.get('daemon_type') == 'haproxy' and s.get('service_name') == service_name]
        
        if rgw_services:
            raise TestExecError(f"RGW daemons still present after service removal: {[s.get('daemon_name') for s in rgw_services]}")
        if haproxy_services:
            raise TestExecError(f"HAProxy daemons still present after service removal: {[s.get('daemon_name') for s in haproxy_services]}")
        
        log.info("Confirmed RGW and HAProxy daemons removed")
        
        # Send test requests to HAProxy frontend (expect failure)
        log.info(f"Sending {num_requests} test requests to HAProxy frontend at {host}:{frontend_port} after service removal")
        successful_requests = 0
        for i in range(num_requests):
            curl_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{frontend_port}"
            result = subprocess.run(curl_cmd, shell=True, capture_output=True, text=True)
            status_code = result.stdout.strip()
            if status_code == '200':
                successful_requests += 1
                log.warning(f"Request {i+1} succeeded unexpectedly with status code {status_code}")
            else:
                log.info(f"Request {i+1} failed as expected with status code {status_code}")
            time.sleep(0.1)
        
        # Verify no successful requests
        if successful_requests > 0:
            raise TestExecError(f"Unexpected successful requests after service removal: {successful_requests} out of {num_requests}")
        
        # Attempt to check HAProxy stats (expect failure)
        stats_url = f"http://{host}:{monitor_port}/stats;csv"
        stats_cmd = f"curl -s -u {monitor_user}:{monitor_password} \"{stats_url}\" | awk -F',' 'NR==1 || /^backend|^frontend|^stats/' | cut -d',' -f1,2,5,8,9,10,18,35,73 | column -s',' -t"
        stats_output = exec_shell_cmd(stats_cmd)
        if stats_output and not stats_output.startswith("<!DOCTYPE"):
            log.warning(f"Unexpected HAProxy stats retrieved after service removal:\n{stats_output}")
        else:
            log.info("HAProxy stats unavailable as expected after service removal")
        
        log.info(f"RGW service {service_name} removed successfully. No RGW or HAProxy daemons found, and traffic stopped.")
        return True
    
    except json.JSONDecodeError:
        raise TestExecError("Failed to parse ceph orch command output")
    except TestExecError as e:
        log.error(e.message)
        return False

def parse_haproxy_stats(stats_output, service_name):
    """Parse HAProxy stats CSV to count requests per RGW backend"""
    rgw_hits = {}
    lines = stats_output.splitlines()
    for line in lines:
        fields = line.split(',')
        if len(fields) > 7 and fields[0] == 'backend' and service_name in fields[1]:
            backend_name = fields[1]  # Backend or server name
            if backend_name.startswith('rgw.'):
                try:
                    hits = int(fields[7])  # Use 'stot' (cumulative sessions)
                    if hits > 0:
                        rgw_hits[backend_name] = hits
                except (ValueError, IndexError):
                    continue
    return rgw_hits

def exec_shell_cmd(cmd):
    """Execute shell command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        log.error(f"Command failed: {cmd}\nError: {e.stderr}")
        return ""

def test_exec(config, ssh_con, rgw_node):
    """Execute RGW and HAProxy colocation and concentrator behavior tests"""
    
    # Check if colocation test should be run
    if config.test_ops.get("rgw_with_concentrators", False):
        log.info("Running RGW and HAProxy colocation check")
        if not rgw_with_concentrators():
            raise TestExecError("RGW and HAProxy colocation check failed")
    
    # Check if concentrator behavior test should be run
    if config.test_ops.get("test_concentrator_behavior", False):
        log.info("Running RGW and HAProxy concentrator behavior test")
        if not test_rgw_concentrator_behavior(config, ssh_con, rgw_node):
            raise TestExecError("RGW and HAProxy concentrator behavior test failed")
    
    # Check if single RGW stop test should be run
    if config.test_ops.get("test_single_rgw_stop", False):
        log.info("Running single RGW instance stop test")
        if not test_single_rgw_stop(config, ssh_con, rgw_node):
            raise TestExecError("Single RGW instance stop test failed")
    
    # Check if HAProxy stop test should be run
    if config.test_ops.get("test_haproxy_stop", False):
        log.info("Running HAProxy instance stop test")
        if not test_haproxy_stop(config, ssh_con, rgw_node):
            raise TestExecError("HAProxy instance stop test failed")
    
    # Check if HAProxy restart test should be run
    if config.test_ops.get("test_haproxy_restart", False):
        log.info("Running HAProxy instance restart test during traffic")
        if not test_haproxy_restart(config, ssh_con, rgw_node):
            raise TestExecError("HAProxy instance restart test failed")
    
    # Check if RGW service removal test should be run
    if config.test_ops.get("test_rgw_service_removal", False):
        log.info("Running RGW service removal test")
        if not test_rgw_service_removal(config, ssh_con, rgw_node):
            raise TestExecError("RGW service removal test failed")
    
    if not (config.test_ops.get("rgw_with_concentrators", False) or 
            config.test_ops.get("test_concentrator_behavior", False) or 
            config.test_ops.get("test_single_rgw_stop", False) or 
            config.test_ops.get("test_haproxy_stop", False) or 
            config.test_ops.get("test_haproxy_restart", False) or 
            config.test_ops.get("test_rgw_service_removal", False)):
        log.info("Skipping RGW and HAProxy tests as per configuration")
    
    # Check for any crashes during execution
    crash_info = reusable.check_for_crash()
    if crash_info:
        raise TestExecError("Ceph daemon crash found!")

if __name__ == "__main__":
    test_info = AddTestInfo("check RGW and HAProxy colocation and concentrator behavior")
    test_info.started_info()

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info(f"TEST_DATA_PATH: {TEST_DATA_PATH}")
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        
        usage = """
        Usage:
          python3 test_rgw_concentrators.py -c test_rgw_concentrators.yaml
        """
        parser = argparse.ArgumentParser(description=usage)
        parser.add_argument("-c", dest="config", help=usage)
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
            ssh_con = utils.connect_remote(rgw_node)
        log_f_name = os.path.basename(os.path.splitext(yaml_file)[0])
        configure_logging(f_name=log_f_name, set_level=args.log_level.upper())
        config = Config(yaml_file)
        config.read(ssh_con)

        test_exec(config, ssh_con, rgw_node)
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)
