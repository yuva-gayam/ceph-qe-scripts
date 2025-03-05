import argparse
import os
import sys
import subprocess
import yaml

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import v1.utils.log as log
from v1.utils.test_desc import AddTestInfo

def execute_command(command):
    """Executes a command and checks for any error (non-zero return code)."""
    process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    return_code = process.returncode

    if return_code != 0:
        log.info(f"Command '{command}' failed with return code {return_code}.")
        return False, stdout, stderr, return_code
    else:
        log.error(f"Command '{command}' succeeded unexpectedly. Stdout: {stdout}, Stderr: {stderr}")
        return True, stdout, stderr, return_code

def test_exec(config):
    test_info = AddTestInfo("test multisite negative secondary")

    try:
        test_info.started_info()

        commands = [
            "radosgw-admin period pull --url http://invalidurl:80 --access-key 21e86bce636c3aa0 --secret cf764951f1fdde5d",
            "radosgw-admin period pull --url http://{node_ip:ceph-pri#node5}:80 --access-key 21e80 --secret dhejsbjans",
            "radosgw-admin period pull --url http://{node_ip:ceph-pri#node5}:80 --access-key 21e86bce636c3aa0 --secret ''",
            "radosgw-admin period pull --url http://{node_ip:ceph-pri#node5}:80 --access-key '' --secret ''",
            "radosgw-admin period pull --url http://{node_ip:ceph-pri#node5}:80 --access-key '' --secret '' --rgw-realm india --rgw-zonegroup shared --rgw-zone secondary",
        ]

        for command in commands:
            success, stdout, stderr, return_code = execute_command(command)
            if success: # if the command returns 0, then the test fails.
                test_info.failed_status(f"Command '{command}' succeeded unexpectedly. Stdout: {stdout}, Stderr: {stderr}, Return Code: {return_code}")
                sys.exit(1)

        test_info.success_status("Negative tests on ceph-sec completed")
        sys.exit(0)

    except Exception as e:
        log.error(f"An error occurred: {e}")
        test_info.failed_status(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RGW Multisite Negative Tests Secondary")
    parser.add_argument("-c", dest="config", help="Test yaml configuration")
    parser.add_argument("--rgw-node", dest="rgw_node", help="rgw node ip") #add this line.
    args = parser.parse_args()

    yaml_file = args.config
    config = {}
    if yaml_file:
        with open(yaml_file, "r") as f:
            config = yaml.safe_load(f)

    test_exec(config)
