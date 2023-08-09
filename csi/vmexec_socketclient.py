"""
Cisco crosswork - 2023
Socket based command execution conduit implementation.
Used to enable the execution of certain mount operations via the crosswork vmexec service, running outside of
the kadalu container.
"""

import os
import json
import socket
import threading
import time
import uuid
import logging
from collections import defaultdict

from kadalulib import (execute, CommandException)

SOCKET_FILE_PATH = "/var/run/vmexec-socket/vmexec.sock"
GLUSTERFS_CMD = "/usr/sbin/glusterfs"
MOUNT_CMD = "/usr/bin/mount"
UNMOUNT_CMD = "/usr/bin/umount"

# List of commands intercepted and sent to the command execution conduit
cmdList = ["glusterfs", "/mount", "/umount", "/fusermount", "losetup", "pgrep"]

is_debug = os.environ.get("DEBUG", "False")


# TODO - Should use the cleanup once volumes are removed
class volume_lock_manager:
    def __init__(self):
        self.locks = defaultdict(threading.Lock)

    def get_lock(self, name):
        return self.locks[name]

    def del_lock(self, name):
        self.locks.pop(name, None)

# Create a named volume lock manager
lock_manager = volume_lock_manager()


def connect_socket(client_socket):
    retry_interval = 5
    max_retries = 12

    retry_count = 0
    connected = False
    while retry_count < max_retries and not connected:
        try:
            # Attempt to connect to the server
            client_socket.connect(SOCKET_FILE_PATH)
            connected = True
            logging.debug("Socket connected to the vmexec!")
            break
        except ConnectionRefusedError:
            # Connection refused, wait for a while before retrying
            logging.debug("Connection refused. Retrying in seconds..." + str(retry_interval))
            time.sleep(retry_interval)
            retry_count += 1

    return connected


def change_log_level(commandList):
    for i in range(len(commandList)):
        if "log-level" in commandList[i] and "DEBUG" in commandList[i][-5:]:
            logging.warning("Changing command debug level from DEBUG to INFO")
            commandList[i] = commandList[i][:-5] + "INFO"
    return commandList


def substitute_cmd(commandList):
    if "/sbin/glusterfs" in commandList[0]:
        commandList[0] = GLUSTERFS_CMD
    elif "/mount" in commandList[0]:
        commandList[0] = MOUNT_CMD
    elif "/umount" in commandList[0]:
        commandList[0] = UNMOUNT_CMD

    if is_debug == "False":
        commandList = change_log_level(commandList)
    return commandList


def socket_client(commandList):
    cmd = substitute_cmd(commandList)
    cmd = " ".join(cmd)

    json_data = {
        "error": "Unable to execute command",
        "result": 1,
        "output": ""
    }

    logging.debug("Connecting socket")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:

        is_connected = connect_socket(client_socket)
        if not is_connected:
            logging.error("Unable to connect to the vmexec server after max retries.")
            json_data["error"] = "Unable to connect to the server for command execution after max retries."
        else:
            logging.debug("Sending the command to socket server for execution")

            data = {
                "id": str(uuid.uuid4()),
                "commandtype": 4,
                "command": cmd,
                "hidden": False,
                "commandtimeout": 100,
                "nodelist": [""]
            }
            json_inp = json.dumps(data)
            client_socket.sendall(json_inp.encode('utf-8'))

            # receive a response from the server
            response = client_socket.recv(1024).decode('utf-8')
            json_data = json.loads(response)
    if len(json_data['error']) != 0:
        raise CommandException(-1, cmd, json_data['error'])
    return json_data['output'], json_data['error'], int(json_data['result'])


def execute_vmexec(*cmd):
    head = cmd[0]
    logging.info(cmd)
    if any(cmdStr in head for cmdStr in cmdList):
        return socket_client(list(cmd))
    else:
        logging.debug("Executing command using default executioner")
        return execute(*cmd)


def is_gl_mount_vmexec(volname, mountpoint):
    args = "bin/glusterfs.*{}.*{}".format(volname, mountpoint)
    out, err, res = execute_vmexec("/usr/bin/pgrep", "-c", "-f", args)
    logging.debug("is_gl_mount_vmexec for volume: %s, returned. out: %s err: %s res: %s", volname, out, err, res)
    return int(out) > 0 and res == 0
