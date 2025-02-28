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
    test_info = AddTestInfo("test multisite negative")

    try:
        test_info.started_info()

        commands = [
            ("radosgw-admin realm create --rgw-realm '' --default", "missing realm name"),
            ("radosgw-admin realm create --rgw-realm india --default", "File exists"), # expecting duplicate error
            ("radosgw-admin zonegroup create --rgw-realm india --rgw-zonegroup '' --endpoints http://{node_ip:node5}:80 --master --default", "zonegroup name not provided"),
            ("radosgw-admin zonegroup create --rgw-realm india --endpoints http://{node_ip:node5}:80 --master --default", "zonegroup name not provided"),
            ("radosgw-admin zonegroup create --rgw-realm india --rgw-zonegroup shared --endpoints http://{node_ip:node5}:80 --master --default", None), # create zonegroup for following tests
            ("radosgw-admin zonegroup create --rgw-realm india --rgw-zonegroup shared --endpoints http://{node_ip:node5}:80 --master --default", "File exists"), # Duplicate zone group
            ("radosgw-admin zone create --rgw-realm india --rgw-zonegroup shared --rgw-zone '' --endpoints http://{node_ip:node5}:80 --master --default", "zone name not provided"),
            ("radosgw-admin zone create --rgw-realm india --rgw-zonegroup shared --rgw-zone primary --endpoints http://{node_ip:node5}:80 --master --default", None), # create zone for following tests
            ("radosgw-admin zone create --rgw-realm india --rgw-zonegroup shared --rgw-zone primary --endpoints http://{node_ip:node5}:80 --master --default", "File exists"), # Duplicate zone
            ("radosgw-admin user create --uid=repuser --display_name='Replication user' --access-key 21e86bce636c3aa0 --secret cf764951f1fdde5d --rgw-realm india --system", "user: repuser exists"),
        ]

        for command, expected_error in commands:
            success, stdout, stderr, return_code = execute_command(command, expected_error)
            if not success:
                test_info.failed_status(f"Command '{command}' failed. Stdout: {stdout}, Stderr: {stderr}, Return Code: {return_code}")
                sys.exit(1)

        test_info.success_status("Negative tests on ceph-pri completed")
        sys.exit(0)

    except Exception as e:
        log.error(f"An error occurred: {e}")
        test_info.failed_status(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RGW Multisite Negative Tests")
    parser.add_argument("-c", dest="config", help="Test yaml configuration")
    args = parser.parse_args()

    yaml_file = args.config
    config = {}
    if yaml_file:
        with open(yaml_file, "r") as f:
            config = yaml.safe_load(f)

    test_exec(config)
