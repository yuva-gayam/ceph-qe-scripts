import argparse
import os
import sys
import subprocess
import yaml

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import v1.utils.log as log
from v1.utils.test_desc import AddTestInfo

def execute_command(command):
    """Executes a command and checks for any error."""
    process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    return_code = process.returncode

    if return_code != 0:
        log.info(f"Command '{command}' failed with return code {return_code}.")
        return False, stdout, stderr, return_code
    else:
        return True, stdout, stderr, return_code

def check_rgw_orphans(rgw_node):
    """Checks for RGW orphans in the specified pool."""

    test_info = AddTestInfo("Check RGW Orphans")

    try:
        test_info.started_info()

        log.info("Running rgw-orphan-list...")
        success, stdout, stderr, return_code = execute_command("rgw-orphan-list")

        if not success:
            test_info.failed_status(f"rgw-orphan-list failed. Stdout: {stdout}, Stderr: {stderr}, Return Code: {return_code}")
            sys.exit(1)

        log.info(stdout)  # Print the available pools

        target_pool = "default.rgw.buckets.data"

        log.info(f"\nChecking for orphans in pool: {target_pool}")
        orphan_check_command = f"rgw-orphan-list --pool={target_pool}"
        success, stdout, stderr, return_code = execute_command(orphan_check_command)

        if not success:
            test_info.failed_status(f"rgw-orphan-list --pool={target_pool} failed. Stdout: {stdout}, Stderr: {stderr}, Return Code: {return_code}")
            sys.exit(1)

        if stdout:
            log.info(stdout)
            if "No orphans found" in stdout:
                log.info(f"No orphans found in pool {target_pool}.")
                test_info.success_status(f"No orphans found in pool {target_pool}.")
                sys.exit(0) # test passes if no orphans
            else:
                log.info(f"Orphans found in pool {target_pool}.")
                test_info.failed_status(f"Orphans found in pool {target_pool}.")
                sys.exit(1) # test fails if orphans found
        else:
            log.info(f"No output from rgw-orphan-list for pool {target_pool}. Possible error or no orphans.")
            test_info.failed_status(f"No output from rgw-orphan-list for pool {target_pool}.")
            sys.exit(1)

    except Exception as e:
        log.error(f"An error occurred: {e}")
        test_info.failed_status(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check RGW Orphans")
    parser.add_argument("--rgw-node", dest="rgw_node", required=True, help="rgw node ip")
    args = parser.parse_args()

    check_rgw_orphans(args.rgw_node)
