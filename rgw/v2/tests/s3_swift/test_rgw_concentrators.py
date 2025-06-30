# test_rgw_concentrators.py (main script)

"""test_rgw_concentrators.py - Test if RGW and HAProxy are on the same node and concentrator behavior

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
    Test S3 operations (create, upload, download, delete) based on config.
    Report status of colocation and concentrator behavior checks
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import traceback
import yaml # Added for io_info file handling
import stat # Added for io_info file permissions
import shutil # Added for directory cleanup

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))

import v2.lib.resource_op as s3lib
from v2.lib.resource_op import Config
from v2.tests.s3_swift.reusables import rgw_concentrators as concentrator_tests
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo
from v2.utils import utils
from v2.tests.s3_swift import reusable
from v2.lib.exceptions import RGWBaseException, TestExecError
# from v2.lib.s3.auth import Auth # Not directly needed here, reusable.get_auth handles it

log = logging.getLogger()

# TEST_DATA_PATH will be initialized in __main__ and made global for test_exec
TEST_DATA_PATH = None # This will be set in __main__

def test_exec(config, ssh_con, rgw_node):
    # Log current working directory for debugging
    current_dir = os.getcwd()
    log.info(f"Current working directory: {current_dir}")

    # Create io_info file in the current working directory
    # This block is kept as per your provided snippet, assuming it's for test tracking.
    log.info("Creating io_info file for user information")
    io_info_file = f"io_info_{os.path.basename(os.path.splitext(config.yaml_file)[0])}.yaml"
    io_info_path = os.path.join(current_dir, io_info_file)
    log.info(f"Attempting to create io_info file at: {io_info_path}")
    
    if not os.path.exists(io_info_path):
        try:
            with open(io_info_path, "w") as fp:
                yaml.safe_dump({"users": []}, fp)
            log.info(f"Successfully created io_info file: {io_info_path}")
            # Set file permissions to ensure readability/writability
            os.chmod(io_info_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
            log.info(f"Set permissions on {io_info_path}: {oct(os.stat(io_info_path).st_mode)[-3:]}")
        except Exception as e:
            log.error(f"Failed to create io_info file {io_info_path}: {str(e)}")
            raise TestExecError(f"Failed to create io_info file: {str(e)}")
    else:
        log.info(f"io_info file already exists: {io_info_path}")
        # Log file permissions
        log.info(f"Existing file permissions: {oct(os.stat(io_info_path).st_mode)[-3:]}")

    # --- Start of Concentrator Specific Tests ---
    if config.test_ops.get("rgw_with_concentrators", False):
        log.info("Running RGW and HAProxy colocation check")
        if not concentrator_tests.rgw_with_concentrators(ssh_con, rgw_node):
            raise TestExecError("RGW and HAProxy colocation check failed")
    
    if config.test_ops.get("test_concentrator_behavior", False):
        log.info("Running RGW and HAProxy concentrator behavior test")
        if not concentrator_tests.test_rgw_concentrator_behavior(config, ssh_con, rgw_node):
            raise TestExecError("RGW and HAProxy concentrator behavior test failed")
    
    if config.test_ops.get("test_single_rgw_stop", False):
        log.info("Running single RGW instance stop test")
        if not concentrator_tests.test_single_rgw_stop(config, ssh_con, rgw_node):
            raise TestExecError("Single RGW instance stop test failed")
    
    if config.test_ops.get("test_haproxy_stop", False):
        log.info("Running HAProxy instance stop test")
        if not concentrator_tests.test_haproxy_stop(config, ssh_con, rgw_node):
            raise TestExecError("HAProxy instance stop test failed")
    
    if config.test_ops.get("test_haproxy_restart", False):
        log.info("Running HAProxy instance restart test during traffic")
        if not concentrator_tests.test_haproxy_restart(config, ssh_con, rgw_node):
            raise TestExecError("HAProxy instance restart test failed")
    
    if config.test_ops.get("test_rgw_service_removal", False):
        log.info("Running RGW service removal test")
        if not concentrator_tests.test_rgw_service_removal(config, ssh_con, rgw_node):
            raise TestExecError("RGW service removal test failed")
    # --- End of Concentrator Specific Tests ---

    # --- Start of S3 Operations (controlled by 'perform_s3_operations' flag) ---
    if config.test_ops.get("perform_s3_operations", False):
        log.info("Running S3 operations (create bucket, create object, download object, delete bucket/object)")

        # Create user(s) for the S3 operations
        user_count = getattr(config, 'user_count', 1)
        log.info(f"Creating {user_count} user(s) for S3 operations.")
        all_users_info = s3lib.create_users(user_count)
        if not all_users_info:
            raise TestExecError(f"Failed to create {user_count} user(s) for S3 operations.")
        
        config.users = all_users_info # Store user info in config for cleanup

        # TEST_DATA_PATH is a global variable initialized in the __main__ block
        # and made accessible here.
        # No 'global TEST_DATA_PATH' needed here, as it's only read, not assigned within this function scope.
        # It's already defined at the module level.

        # Ensure TEST_DATA_PATH is a valid directory before passing
        if TEST_DATA_PATH is None:
            # Fallback or error if TEST_DATA_PATH wasn't set in __main__
            raise TestExecError("TEST_DATA_PATH was not properly initialized in __main__.")
        
        # Call the perform_s3_operations function from concentrator_tests module
        # This function will handle create_bucket, create_object, download_object, delete_bucket_object
        if not concentrator_tests.perform_s3_operations(config, ssh_con, all_users_info, TEST_DATA_PATH):
            raise TestExecError("S3 operations failed.")

        # Clean up users after test
        if hasattr(config, 'users'):
            for each_user in config.users:
                log.info(f"Deleting user: {each_user['user_id']}")
                reusable.remove_user(each_user)
                log.info(f"User deleted: {each_user['user_id']}")
    # --- End of S3 Operations ---

    # This condition now checks if ANY of the concentrator tests OR S3 operations were enabled.
    if not (
        config.test_ops.get("rgw_with_concentrators", False)
        or config.test_ops.get("test_concentrator_behavior", False)
        or config.test_ops.get("test_single_rgw_stop", False)
        or config.test_ops.get("test_haproxy_stop", False)
        or config.test_ops.get("test_haproxy_restart", False)
        or config.test_ops.get("test_rgw_service_removal", False)
        or config.test_ops.get("perform_s3_operations", False) # Updated to new flag
    ):
        log.info("Skipping RGW and HAProxy tests and S3 operations as per configuration")
    
    # Check for any crashes during execution
    crash_info = reusable.check_for_crash()
    if crash_info:
        raise TestExecError("Ceph daemon crash found!")


if __name__ == "__main__":
    test_info = AddTestInfo("check RGW and HAProxy colocation and concentrator behavior and S3 operations")
    test_info.started_info()

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir_name = "rgw_test_data" # A specific name for test data
        
        # This is where TEST_DATA_PATH is actually assigned its value.
        # No 'global' keyword needed here as we are at the module's top level (within __main__).
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir_name) 
        
        log.info(f"TEST_DATA_PATH calculated and set to: {TEST_DATA_PATH}")

        # Ensure the test data directory exists and is clean
        if os.path.exists(TEST_DATA_PATH):
            log.info(f"Clearing existing test data directory: {TEST_DATA_PATH}")
            shutil.rmtree(TEST_DATA_PATH)
        log.info(f"Creating test data directory: {TEST_DATA_PATH}")
        os.makedirs(TEST_DATA_PATH, exist_ok=True)
        
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
        config.yaml_file = yaml_file  # Store yaml_file for io_info filename generation

        test_exec(config, ssh_con, rgw_node)
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)
    finally:
        # Optional: Clean up test data directory after test completion (success or failure)
        if TEST_DATA_PATH and os.path.exists(TEST_DATA_PATH):
            log.info(f"Cleaning up test data directory: {TEST_DATA_PATH}")
            shutil.rmtree(TEST_DATA_PATH)
