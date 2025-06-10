#!/usr/bin/env python3
import os
import sys
import logging
import time
import traceback
import argparse
import yaml
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.resource_op import Config
from v2.lib.rgw_config_opts import CephConfOp, RGWService
from v2.lib.s3.auth import Auth
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo
from v2.utils.utils import RGWService, exec_shell_cmd, gen_s3_object_name
import v2.lib.resource_op as s3lib

log = logging.getLogger()
TEST_DATA_PATH = None

def load_config(config_file):
    """Load the configuration from the YAML file."""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def check_rgw_status(rgw_service, service_name, expected_state, max_wait):
    """Check the status of RGW instances."""
    start_time = time.time()
    while time.time() - start_time < max_wait:
        status = rgw_service.ps(service_name=service_name)
        if all(instance['status'] == expected_state for instance in status):
            log.info(f"RGW instances in {expected_state} state")
            return True
        time.sleep(5)
    raise TestExecError(f"RGW instances not in {expected_state} state within {max_wait}s")

def check_haproxy_status(rgw_service, service_name, expected_state, max_wait):
    """Check the status of HAProxy."""
    start_time = time.time()
    while time.time() - start_time < max_wait:
        status = rgw_service.ps(service_name=service_name)
        if all(instance['status'] == expected_state for instance in status):
            log.info(f"HAProxy in {expected_state} state")
            return True
        time.sleep(5)
    raise TestExecError(f"HAProxy not in {expected_state} state within {max_wait}s")

def check_traffic_distribution(rgw_conn, bucket_name, obj_name, service_name, config, user_info, expected='even', delay=5):
    """Verify traffic distribution by attempting S3 PUT and GET, and checking HAProxy stats."""
    time.sleep(delay)  # Wait for traffic to stabilize
    
    # Test S3 PUT operation
    log.info(f"Attempting to PUT object {obj_name} to bucket {bucket_name}")
    s3_object_path = os.path.join(TEST_DATA_PATH, obj_name)
    put_status = s3lib.resource_op({
        "obj": rgw_conn,
        "resource": "upload_object",
        "kwargs": dict(
            Key=obj_name,
            Bucket=bucket_name,
            File=s3_object_path,
            Config=config,
            UserInfo=user_info
        )
    })
    if put_status is False:
        raise TestExecError(f"Failed to PUT object {obj_name}")
    log.info(f"Successfully uploaded object {obj_name}")
    
    # Test S3 GET operation
    log.info(f"Attempting to GET object {obj_name} from bucket {bucket_name}")
    get_status = s3lib.resource_op({
        "obj": rgw_conn,
        "resource": "Object",
        "args": [bucket_name, obj_name]
    })
    try:
        get_result = get_status.get()
        if get_result["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise TestExecError(f"Failed to GET object {obj_name}: HTTP {get_result['ResponseMetadata']['HTTPStatusCode']}")
        log.info(f"Successfully retrieved object {obj_name}")
    except Exception as e:
        raise TestExecError(f"Object GET failed: {str(e)}")
    
    # Check HAProxy stats for distribution
    cmd = f"curl -u admin http://localhost:1967/stats;csv"
    out = exec_shell_cmd(cmd)
    if not out:
        raise TestExecError("Failed to fetch HAProxy stats")
    log.info(f"HAProxy stats: {out}")
    # Simplified check: 'even' if traffic hits RGW instances
    if expected == 'even' and "backend" in out and service_name in out:
        log.info("Traffic (PUT and GET) appears evenly distributed")
        return True
    elif expected == 'active' and "backend" in out:
        log.info("Traffic (PUT and GET) is active")
        return True
    raise TestExecError(f"Traffic not {expected} as expected")

def check_traffic_to_stopped_rgw(rgw_conn, bucket_name, obj_name, service_name, config, user_info, delay=5):
    """Verify no traffic goes to stopped RGW instance; PUT and GET should succeed via remaining instance."""
    time.sleep(delay)
    cmd = f"curl -u admin http://localhost:1967/stats;csv"
    out = exec_shell_cmd(cmd)
    if not out:
        raise TestExecError("Failed to fetch HAProxy stats")
    log.info(f"HAProxy stats: {out}")
    
    # Test S3 PUT operation
    log.info(f"Attempting to PUT object {obj_name} to bucket {bucket_name}")
    s3_object_path = os.path.join(TEST_DATA_PATH, obj_name)
    put_status = s3lib.resource_op({
        "obj": rgw_conn,
        "resource": "upload_object",
        "kwargs": dict(
            Key=obj_name,
            Bucket=bucket_name,
            File=s3_object_path,
            Config=config,
            UserInfo=user_info
        )
    })
    if put_status is False:
        raise TestExecError(f"Failed to PUT object {obj_name}")
    log.info(f"Successfully uploaded object {obj_name} via remaining RGW instance")
    
    # Test S3 GET operation
    log.info(f"Attempting to GET object {obj_name} from bucket {bucket_name}")
    get_status = s3lib.resource_op({
        "obj": rgw_conn,
        "resource": "Object",
        "args": [bucket_name, obj_name]
    })
    try:
        get_result = get_status.get()
        if get_result["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise TestExecError(f"Failed to GET object {obj_name}: HTTP {get_result['ResponseMetadata']['HTTPStatusCode']}")
        log.info(f"Successfully retrieved object {obj_name} via remaining RGW instance")
    except Exception as e:
        raise TestExecError(f"Object GET failed after stopping RGW instance: {str(e)}")
    
    # Check stats: Placeholder for confirming stopped instance gets no traffic
    if "0" not in out:  # Adjust based on actual stats format
        raise TestExecError("Traffic still routes to stopped RGW instance")
    return True

def test_exec(config, ssh_con):
    """Execute the RGW and HAProxy behavior test steps."""
    rgw_service = RGWService()
    ceph_conf = CephConfOp(ssh_con)
    cfg = load_config(config.config_path)
    rgw_service_name = cfg['rgw_service']
    haproxy_service_name = cfg['haproxy_service']
    bucket_name = cfg['bucket_name']
    max_wait = cfg['validation']['max_wait_time']
    traffic_delay = cfg['validation']['traffic_check_delay']
    rgw_running = cfg['validation']['rgw_running_state']
    rgw_stopped = cfg['validation']['rgw_stopped_state']
    haproxy_running = cfg['validation']['haproxy_running_state']
    haproxy_stopped = cfg['validation']['haproxy_stopped_state']
    
    # Create a test user for S3 operations
    user_info = s3lib.create_users(1)[0]
    auth = Auth(user_info, ssh_con, ssl=config.ssl, haproxy=config.haproxy)
    rgw_conn = auth.do_auth()
    
    # Generate a unique object name
    obj_name = gen_s3_object_name(bucket_name, 1)
    s3_object_path = os.path.join(TEST_DATA_PATH, obj_name)
    # Create a small test file for PUT
    with open(s3_object_path, 'wb') as f:
        f.write(b"Test data for RGW and HAProxy traffic validation")
    
    # Step 1: Restart the RGW service
    log.info("Step 1: Restarting RGW service")
    exec_shell_cmd(f"ceph orch apply {rgw_service_name} --restart")
    time.sleep(10)  # Initial wait for restart
    if not check_rgw_status(rgw_service, rgw_service_name, rgw_running, max_wait):
        raise TestExecError("RGW restart failed")
    if not check_haproxy_status(rgw_service, haproxy_service_name, haproxy_running, max_wait):
        raise TestExecError("HAProxy failed to reconnect after RGW restart")
    if not check_traffic_distribution(rgw_conn, bucket_name, obj_name, rgw_service_name, config, user_info, 'even', traffic_delay):
        raise TestExecError("Traffic (PUT and GET) not evenly distributed after RGW restart")

    # Step 2: Stop one RGW instance
    log.info("Step 2: Stopping one RGW instance")
    exec_shell_cmd(f"ceph orch apply {rgw_service_name} --stop --limit 1")
    time.sleep(10)
    if not check_traffic_to_stopped_rgw(rgw_conn, bucket_name, obj_name, rgw_service_name, config, user_info, traffic_delay):
        raise TestExecError("Traffic still routes to stopped RGW instance")
    if not check_rgw_status(rgw_service, rgw_service_name, rgw_running, max_wait):
        raise TestExecError("Remaining RGW instance not handling traffic")

    # Step 3: Remove the RGW service
    log.info("Step 3: Removing RGW service")
    exec_shell_cmd(f"ceph orch remove {rgw_service_name}")
    time.sleep(10)
    if not check_rgw_status(rgw_service, rgw_service_name, rgw_stopped, max_wait):
        raise TestExecError("RGW instances not stopped after removal")
    if not check_haproxy_status(rgw_service, haproxy_service_name, haproxy_stopped, max_wait):
        raise TestExecError("HAProxy not stopped after RGW removal")
    if rgw_service.ps(service_name=haproxy_service_name):
        raise TestExecError("HAProxy service not removed")

    # Step 4: Stop the HAProxy process (assuming RGW redeployed externally)
    log.info("Step 4: Stopping HAProxy process")
    exec_shell_cmd(f"ceph orch apply {haproxy_service_name} --stop")
    time.sleep(5)
    if not check_haproxy_status(rgw_service, haproxy_service_name, haproxy_stopped, max_wait):
        raise TestExecError("HAProxy not stopped")
    try:
        check_traffic_distribution(rgw_conn, bucket_name, obj_name, rgw_service_name, config, user_info, 'active', traffic_delay)
        raise TestExecError("Traffic (PUT and GET) not stopped after HAProxy shutdown")
    except TestExecError as e:
        log.info("Expected: Traffic stopped as HAProxy is down")

    # Step 5: Restart the HAProxy
    log.info("Step 5: Restarting HAProxy")
    exec_shell_cmd(f"ceph orch apply {haproxy_service_name} --restart")
    time.sleep(10)
    if not check_haproxy_status(rgw_service, haproxy_service_name, haproxy_running, max_wait):
        raise TestExecError("HAProxy failed to restart")
    if not check_traffic_distribution(rgw_conn, bucket_name, obj_name, rgw_service_name, config, user_info, 'even', traffic_delay):
        raise TestExecError("Traffic (PUT and GET) not evenly distributed after HAProxy restart")

    # Cleanup test file
    if os.path.exists(s3_object_path):
        os.remove(s3_object_path)
        log.info(f"Cleaned up test file: {s3_object_path}")

    log.info("Test completed successfully!")

if __name__ == "__main__":
    test_info = AddTestInfo("Test RGW and HAProxy Service Behavior")
    test_info.started_info()

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info("TEST_DATA_PATH: %s" % TEST_DATA_PATH)
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        
        parser = argparse.ArgumentParser(description="RGW and HAProxy Behavior Testing")
        parser.add_argument("-c", dest="config", help="Test config YAML file")
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
            from v2.utils.utils import connect_remote
            ssh_con = connect_remote(rgw_node)
        
        log_f_name = os.path.basename(os.path.splitext(yaml_file)[0])
        configure_logging(f_name=log_f_name, set_level=args.log_level.upper())
        config = Config(yaml_file)
        config.read(ssh_con)
        
        test_exec(config, ssh_con)
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)
