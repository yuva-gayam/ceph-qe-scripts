import configparser
import datetime
import hashlib
import json
import logging
import os
import random
import shutil
import socket
import string
import subprocess
import time
from random import randint
from re import S
from urllib.parse import urlparse

import botocore
import paramiko
import yaml
from v2.lib.exceptions import SyncFailedError, TestExecError

BUCKET_NAME_PREFIX = "bucky" + "-" + str(random.randrange(1, 5000))
S3_OBJECT_NAME_PREFIX = "key"
log = logging.getLogger()


def exec_long_running_shell_cmd(cmd):
    try:
        log.info("executing cmd: %s" % cmd)
        pr = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            shell=True,
        )
        # Poll process.stdout to show stdout live
        while True:
            output = pr.stdout.readline()
            if pr.poll() is not None:
                break
            if output:
                log.info(output.strip())
        print()
        rc = pr.poll()
        if rc == 0:
            log.info("cmd excuted")
            return True
        else:
            raise Exception("error occured \nreturncode: %s" % (rc))
    except Exception as e:
        log.error("cmd execution failed")
        log.error(e)
        get_crash_log()
        return False


def exec_shell_cmd(cmd, debug_info=False, return_err=False):
    try:
        log.info("executing cmd: %s" % cmd)
        pr = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=False,
            shell=True,
        )
        out, err = pr.communicate()
        out = out.decode("utf-8", errors="ignore")
        err = err.decode("utf-8", errors="ignore")
        if pr.returncode == 0:
            log.info("cmd excuted")
            if out is not None:
                log.info(out)
                if debug_info == True:
                    log.info(err)
                    return out, err
                else:
                    return out
        else:
            if return_err == True:
                return err
            raise Exception(
                f"stderr: {err} \nreturncode: {pr.returncode} \nstdout:{out}"
            )
    except Exception as e:
        log.error("cmd execution failed")
        log.error(e)
        get_crash_log()
        return False


def connect_remote(rgw_host, user_nm="cephuser", passw="cephuser"):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(rgw_host, port=22, username=user_nm, password=passw, timeout=3)
    if ssh is None:
        raise Exception("Connection with remote machine failed")
    else:
        return ssh


def remote_exec_shell_cmd(ssh, cmd, return_output=False):
    try:
        log.info("executing cmd on remote node: %s" % cmd)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        cmd_output = stdout.read().decode()
        cmd_error = stderr.read().decode()
        log.info(cmd_output)
        if len(cmd_error) == 0:
            if return_output:
                return cmd_output
            else:
                return True
        else:
            log.error(cmd_error)
            return False
    except Exception as e:
        log.error("cmd execution failed on remote machine")
        log.error(e)
        get_crash_log()
        return False


def get_crash_log():
    # dump the crash log information on to the console, if any
    _, ceph_version_name = get_ceph_version()
    if ceph_version_name in ["luminous", "nautilus"]:
        crash_path = "sudo ls -t /var/lib/ceph/crash/*/log | head -1"
    else:
        crash_path = "sudo ls -t /var/lib/ceph/*/crash/*/log | head -1"
    out = exec_shell_cmd(crash_path)
    crash_file = out.rstrip("\n")
    if os.path.isfile(crash_file):
        cmd = f"cat {crash_file}"
        exec_shell_cmd(cmd)


def get_md5(fname):
    log.info("fname: %s" % fname)
    return hashlib.md5(open(fname, "rb").read()).hexdigest()
    # return "@424242"


def get_file_size(min, max):
    size = lambda x: x if x % 5 == 0 else size(randint(min, max))
    return size(randint(min, max))


def create_file(fname, size):
    # give the size in mega bytes.
    file_size = 1024 * 1024 * size
    with open(fname, "wb") as f:
        f.truncate(file_size)
    fname_with_path = os.path.abspath(fname)
    # md5 = get_md5(fname)
    return fname_with_path


def split_file(fname, size_to_split=5, splitlocation=""):
    # size_to_split should be in MBs
    split_cmd = (
        "split" + " " + "-b" + str(size_to_split) + "m " + fname + " " + splitlocation
    )
    exec_shell_cmd(split_cmd)


def cleanup_test_data_path(test_data_path):
    """
    Deletes all files and directories in mentioned test_data_path
    Args:
        test_data_path(str): Test data path
    """
    for filename in os.listdir(test_data_path):
        file_path = os.path.join(test_data_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            log.error("Failed to delete %s. Reason: %s" % (file_path, e))


class FileOps(object):
    def __init__(self, filename, type):
        self.type = type
        self.fname = filename

    def get_data(self):
        data = None
        with open(self.fname, "r") as fp:
            if self.type == "json":
                data = json.load(fp)
            if self.type == "txt" or self.type == "ceph.conf":
                raw_data = fp.readlines()
                tmp = lambda x: x.rstrip("\n")
                data = list(map(tmp, raw_data))
            if self.type == "yaml":
                data = yaml.safe_load(fp)
        fp.close()
        return data

    def add_data(self, data, ssh_con=None):
        with open(self.fname, "w") as fp:
            if self.type == "json":
                json.dump(data, fp, indent=4)
            if self.type == "txt":
                fp.write(data)
            if self.type == "ceph.conf":
                if ssh_con is not None:
                    destination = "/etc/ceph/ceph.conf"
                    data.write(fp)
                    fp.close()
                    sftp_client = ssh_con.open_sftp()
                    sftp_client.put(self.fname, destination)
                    sftp_client.close()
                else:
                    data.write(fp)
            elif self.type is None:
                data.write(fp)
            elif self.type == "yaml":
                yaml.dump(data, fp, default_flow_style=False)
        fp.close()


class ConfigParse(object):
    def __init__(self, fname, ssh_con=None):
        self.fname = fname
        self.cfg = configparser.ConfigParser()
        if ssh_con is not None:
            tmp_file = fname + ".rgw.tmp"
            sftp_client = ssh_con.open_sftp()
            fname = sftp_client.get(fname, tmp_file)
            sftp_client.close()
            self.cfg.read(tmp_file)
            self.fname = tmp_file
        else:
            self.cfg.read(fname)

    def set(self, section, option, value=None):
        self.cfg.set(section, option, value)
        return self.cfg

    def add_section(self, section):
        try:
            self.cfg.add_section(section)
            return self.cfg
        except configparser.D
