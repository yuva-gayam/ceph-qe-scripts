import json
import os
import sys
import re
from datetime import datetime
import logging
import time

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../")))

import v2.utils.utils as utils
from v2.lib.exceptions import SyncFailedError

log = logging.getLogger(__name__)


def sync_status(retry=25, delay=60, ssh_con=None, return_while_sync_inprogress=False):
    """
    verify multisite sync status and recover if stuck
    """

    def get_sync_status():
        if ssh_con:
            stdin, stdout, stderr = ssh_con.exec_command("sudo radosgw-admin sync status")
            cmd_error = stderr.read().decode()
            if cmd_error:
                log.error(f"error: {cmd_error}")
            return stdout.read().decode()
        else:
            return utils.exec_shell_cmd("sudo radosgw-admin sync status")

    def restart_rgw():
        log.info("Restarting all RGW daemons using utils.restart_rgw...")
        utils.restart_rgw(restart_all=True, ssh_con=ssh_con)
        time.sleep(60)

    def force_sync_init():
        log.info("Forcing metadata full sync on archive zone")
        cmd = "sudo radosgw-admin sync init --rgw-zone=archive"
        if ssh_con:
            ssh_con.exec_command(cmd)
        else:
            utils.exec_shell_cmd(cmd)
        time.sleep(60)

    log.info("Checking sync status...")
    check_sync_status = get_sync_status()

    if not check_sync_status:
        raise AssertionError("Sync status output is empty")

    log.info(f"sync status op is: {check_sync_status}")

    # Check for 'failed' or 'ERROR'
    if "failed" in check_sync_status or "ERROR" in check_sync_status:
        log.info("Detected 'failed' in sync status. Waiting for 70 seconds before rechecking...")
        time.sleep(70)
        check_sync_status = get_sync_status()
        log.info(f"Rechecked sync status after wait: {check_sync_status}")

        if "failed" in check_sync_status or "ERROR" in check_sync_status:
            log.error("sync is in failed or errored state")
            raise SyncFailedError("sync status is in failed or errored state!")

    log.info(f"Retrying sync check up to {retry} times with {delay}s between retries")
    for retry_count in range(retry):
        if "metadata is behind" not in check_sync_status:
            log.info("Sync looks complete or no lag")
            break

        log.info(f"[Retry {retry_count+1}/{retry}] Metadata sync still behind. Sleeping for {delay}s...")
        time.sleep(delay)
        check_sync_status = get_sync_status()
        log.info(f"sync status op is: {check_sync_status}")

        if "metadata is behind" not in check_sync_status:
            log.info("Metadata sync caught up.")
            break

        # Check if metadata sync is actually stuck (lag older than 5 min)
        try:
            current_time_match = re.search(r"current time\s+([^\s]+)", check_sync_status)
            oldest_change_match = re.search(
                r"oldest incremental change not applied: ([^\s]+)", check_sync_status
            )

            if current_time_match and oldest_change_match:
                current_time_str = current_time_match.group(1).replace("Z", "")
                oldest_change_str = oldest_change_match.group(1)

                current_time = datetime.strptime(current_time_str, "%Y-%m-%dT%H:%M:%S")
                oldest_time = datetime.strptime(oldest_change_str, "%Y-%m-%dT%H:%M:%S.%f+0000")

                lag = (current_time - oldest_time).total_seconds()
                log.info(f"Metadata sync lag is {lag:.2f} seconds")

                if lag < 300:
                    continue  # Still within acceptable sync window

        except Exception as e:
            log.warning(f"Could not parse timestamps: {e}")
            # If timestamp parsing fails, proceed conservatively
            continue

    else:
        log.error("Metadata sync stuck after retries, starting recovery...")

        # Step 1: Restart RGW
        restart_rgw()

        # Step 2: Check again after restart
        check_sync_status = get_sync_status()
        if "metadata is behind" in check_sync_status:
            log.warning("Sync still behind after RGW restart. Forcing full sync.")

            # Step 3: Force full sync
            force_sync_init()
            check_sync_status = get_sync_status()

            if "metadata is behind" in check_sync_status:
                raise SyncFailedError("Metadata sync is stuck after RGW restart and full sync init.")

    # Final verification
    if "data is caught up with source" in check_sync_status:
        log.info("Data sync is complete")
    elif "archive" in check_sync_status or "not syncing from zone" in check_sync_status:
        log.info("Archive zone is not designed to sync data to source zone — skipping")
    else:
        raise SyncFailedError("sync is either slow or stuck")

    # check for cluster health status and omap if any
    check_ceph_status()


def check_ceph_status():
    """
    get the ceph cluster status and health
    """
    log.info("get ceph status")
    ceph_status = utils.exec_shell_cmd(cmd="sudo ceph status")
    if "HEALTH_ERR" in ceph_status or "large omap objects" in ceph_status:
        raise Exception(
            "ceph status is either in HEALTH_ERR or we have large omap objects."
        )
    else:
        log.info("ceph status - HEALTH_OK")
