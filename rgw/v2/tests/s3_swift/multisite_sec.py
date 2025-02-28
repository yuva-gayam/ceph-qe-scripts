import os
import sys
import subprocess
import yaml

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))
import v1.utils.log as log
from v1.utils.test_desc import AddTestInfo

def execute_command(command, expected_error=None):
    """Executes a command and checks for expected errors."""
    process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    return_code = process.returncode

    if expected_error:
        if expected_error in stderr or expected_error in stdout:
            log.info(f"Expected error '{expected_error}' found in output.")
            return True, stdout, stderr, return_code
        else:
            log.error(f"Expected error '{expected_error}' not found. Output:\nStdout: {stdout}\nStderr: {stderr}")
            return False, stdout, stderr, return_code
    else:
        return True, stdout, stderr, return_code

def test_exec(config):
    test_info = AddTestInfo("test multisite negative secondary")

    try:
        test_info.started_info()

        commands = [
            ("radosgw-admin period pull --url http://invalidurl:80 --access-key 21e86bce636c3aa0 --secret cf764951f1fdde5d", "Unknown error 2200"),
            ("radosgw-admin period pull --url http://{node_ip:ceph-pri#node5}:80 --access-key 21e80 --secret dhejsbjans", "Permission denied"),
            ("radosgw-admin period pull --url http://{node_ip:ceph-pri#node5}:80 --access-key 21e86bce636c3aa0 --secret ''", "An --access-key and --secret must be provided with --url."),
            ("radosgw-admin period pull --url http://{node_ip:ceph-pri#node5}:80 --access-key '' --secret ''", "An --access-key and --secret must be provided with --url."),
            ("radosgw-admin period pull --url http://{node_ip:ceph-pri#node5}:80 --access-key '' --secret '' --rgw-realm india --rgw-zonegroup shared --rgw-zone secondary", "An --access-key and --secret must be provided with --url."),
        ]

        for command, expected_error in commands:
            success, stdout, stderr, return_code = execute_command(command, expected_error)
            if not success:
                test_info.failed_status(f"Command '{command}' failed. Stdout: {stdout}, Stderr: {stderr}, Return Code: {return_code}")
                sys.exit(1)

        test_info.success_status("Negative tests on ceph-sec completed")
        sys.exit(0)

    except Exception as e:
        log.error(f"An error occurred: {e}")
        test_info.failed_status(f"An error occurred: {e}")
        sys
