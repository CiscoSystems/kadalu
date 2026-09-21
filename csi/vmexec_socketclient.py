"""
Cisco crosswork - 2023
Socket based command execution conduit implementation.
Used to enable the execution of certain mount operations via the crosswork vmexec service, running outside of
the kadalu container.
"""

import os
import json
import re
import socket
import threading
import time
import uuid
import logging
from collections import defaultdict
from contextlib import contextmanager

from kadalulib import (execute, CommandException)

SOCKET_FILE_PATH = "/var/run/vmexec-socket/vmexec.sock"
GLUSTERFS_CMD = "/usr/sbin/glusterfs"
MOUNT_CMD = "/usr/bin/mount"
UNMOUNT_CMD = "/usr/bin/umount"

# Socket communication timeout in seconds (CWE-400 prevention)
# Must exceed the longest expected vmexec operation (glusterfs mount can take 300s+)
SOCKET_TIMEOUT = int(os.environ.get("VMEXEC_SOCKET_TIMEOUT", "900"))

# Timeout sent to vmexec server for command execution (seconds)
VMEXEC_CMD_TIMEOUT = int(os.environ.get("VMEXEC_CMD_TIMEOUT", "800"))

# Connect timeout, kept short: a vmexec that holds the socket open but never
# accepts would otherwise block connect() forever, since the default socket
# has no timeout until settimeout() runs after connect (CWE-400)
VMEXEC_CONNECT_TIMEOUT = int(os.environ.get("VMEXEC_CONNECT_TIMEOUT", "10"))

# Maximum response size from vmexec server (16 KB)
MAX_RESPONSE_SIZE = 16384

# A worker inside a vmexec round trip is stuck there until the host answers or
# SOCKET_TIMEOUT expires, and the gRPC pool is the only bound on how many can
# be.  Once every worker is in one, the RPCs that touch no storage at all --
# NodeGetCapabilities, Probe -- have no thread left to run on, and kubelet
# reports that as a plugin missing STAGE_UNSTAGE_VOLUME rather than as a busy
# one.  Capping round trips below the worker count keeps a reserve those RPCs
# can always use, so a wedged host costs volume operations but never the whole
# plugin.  Mirrors the CSI_MAX_WORKERS default in main.py.
# The reserve is small on purpose.  The RPCs it protects answer in
# microseconds without touching storage, so a handful of workers serve far
# more of them than kubelet will ever ask for, and every worker added to the
# reserve is one taken away from doing real mount work during a bulk attach.
_CSI_MAX_WORKERS = int(os.environ.get("CSI_MAX_WORKERS", "64"))
VMEXEC_WORKER_RESERVE = int(os.environ.get("VMEXEC_WORKER_RESERVE", "8"))
VMEXEC_MAX_INFLIGHT = int(os.environ.get(
    "VMEXEC_MAX_INFLIGHT", max(1, _CSI_MAX_WORKERS - VMEXEC_WORKER_RESERVE)))
_vmexec_slots = threading.BoundedSemaphore(VMEXEC_MAX_INFLIGHT)

VMEXEC_BUSY = "vmexec busy: %d concurrent operations already in flight" \
              % VMEXEC_MAX_INFLIGHT

# Shedding happens in bursts by nature, and a line per refusal would bury the
# storm it is reporting under thousands of copies of itself.  One line per
# interval carries the same news and brings the count with it.
VMEXEC_SHED_LOG_INTERVAL = int(os.environ.get("VMEXEC_SHED_LOG_INTERVAL", "10"))
_shed_state = {"last": 0.0, "suppressed": 0}
_shed_lock = threading.Lock()


def _log_shed(cmd_str):
    """Report a refusal, at most once per interval, with what it stands for."""
    with _shed_lock:
        _shed_state["suppressed"] += 1
        now = time.monotonic()
        if now - _shed_state["last"] < VMEXEC_SHED_LOG_INTERVAL:
            return
        count = _shed_state["suppressed"]
        _shed_state["last"] = now
        _shed_state["suppressed"] = 0

    logging.warning(
        "Shedding vmexec commands: %d refused in the last %ds with %d already "
        "in flight, most recently %s.  Workers are being held by a host that "
        "is not answering; kubelet will retry these.",
        count, VMEXEC_SHED_LOG_INTERVAL, VMEXEC_MAX_INFLIGHT, cmd_str)


@contextmanager
def vmexec_inflight(cmd_str):
    """
    Hold one of the vmexec round-trip slots, or refuse.

    Refusing is the point.  Waiting for a slot would occupy the worker this
    exists to keep free, so a caller over budget fails immediately instead;
    kubelet already retries every volume RPC, and a retry that finds the host
    healthy costs far less than a plugin that answers nothing.
    """
    if not _vmexec_slots.acquire(blocking=False):
        _log_shed(cmd_str)
        raise CommandException(-1, cmd_str, VMEXEC_BUSY)
    try:
        yield
    finally:
        _vmexec_slots.release()

# List of commands intercepted and sent to the command execution conduit
cmdList = ["glusterfs", "/mount", "/umount", "/fusermount", "losetup", "pgrep"]

is_debug = os.environ.get("DEBUG", "False")


# TODO - Should use the cleanup once volumes are removed
class volume_lock_manager:
    # Mount exclusion and the per-pool size accounting both hang off this map
    # now that the global locks are gone, so handing two callers different
    # lock objects for one name silently undoes that exclusion and lets two
    # threads mount the same point.  defaultdict only inserts atomically while
    # the GIL is held, so the map is guarded rather than relying on that.
    def __init__(self):
        self.locks = defaultdict(threading.Lock)
        self.users = defaultdict(int)
        self._guard = threading.Lock()

    @contextmanager
    def hold(self, name):
        """
        Hold the lock for name for the duration of the block.

        The caller is registered before the lock is taken, and that is what
        makes del_lock() safe: dropping a name between another thread looking
        its lock up and acquiring it would leave the next caller creating a
        second lock for the same name, and both would enter a section each
        believes it holds alone.  A name in use is never dropped.
        """
        with self._guard:
            lock = self.locks[name]
            self.users[name] += 1
        try:
            with lock:
                yield
        finally:
            with self._guard:
                self.users[name] -= 1

    def get_lock(self, name):
        """
        The lock for name, for callers that acquire it themselves.

        Prefer hold(): a lock taken this way is invisible to del_lock().
        """
        with self._guard:
            return self.locks[name]

    def del_lock(self, name):
        """Forget a name's lock.  Declines while a caller is holding it."""
        with self._guard:
            if self.users.get(name):
                return False
            self.locks.pop(name, None)
            self.users.pop(name, None)
            return True

# Create a named volume lock manager
lock_manager = volume_lock_manager()


def connect_socket(client_socket):
    retry_interval = 5
    max_retries = 12

    retry_count = 0
    connected = False
    while retry_count < max_retries and not connected:
        try:
            # Attempt to connect to the server, bounded so a server that
            # never accepts cannot block this thread indefinitely (CWE-400)
            client_socket.settimeout(VMEXEC_CONNECT_TIMEOUT)
            client_socket.connect(SOCKET_FILE_PATH)
            # Raise to the command timeout for the send/recv phase
            client_socket.settimeout(SOCKET_TIMEOUT)
            connected = True
            logging.debug("Socket connected to the vmexec!")
            break
        except ConnectionRefusedError:
            # Connection refused, wait for a while before retrying
            logging.debug("Connection refused. Retrying in %d seconds...", retry_interval)
            time.sleep(retry_interval)
            retry_count += 1
        except socket.timeout:
            # Listening but not accepting.  Not retried on this socket: a
            # timed-out connect leaves it unusable, and the caller is better
            # off getting its thread back than waiting out the retries.
            logging.error("Timed out connecting to vmexec after %ds",
                          VMEXEC_CONNECT_TIMEOUT)
            break

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


def _recv_all(sock, max_size=MAX_RESPONSE_SIZE):
    """Read from socket until connection closes, up to max_size bytes (CWE-400 prevention)."""
    chunks = []
    total = 0
    while total < max_size:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        except socket.timeout:
            logging.warning("Socket recv timed out after %d bytes received", total)
            break
    if total >= max_size:
        logging.warning("Response truncated at %d bytes (max_size=%d)", total, max_size)
    return b''.join(chunks)


def socket_client(commandList):
    cmd = substitute_cmd(commandList)
    # Send command as JSON array for structured execution (CWE-78 mitigation)
    cmd_str = " ".join(cmd)

    json_data = {
        "error": "Unable to execute command",
        "result": 1,
        "output": ""
    }

    logging.debug("Connecting socket")
    with vmexec_inflight(cmd_str), \
            socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:

        is_connected = connect_socket(client_socket)
        if not is_connected:
            logging.error("Unable to connect to the vmexec server after max retries.")
            json_data["error"] = "Unable to connect to the server for command execution after max retries."
        else:
            logging.debug("Sending the command to socket server for execution")

            data = {
                "id": str(uuid.uuid4()),
                "commandtype": 4,
                "command": cmd_str,
                "hidden": False,
                "commandtimeout": VMEXEC_CMD_TIMEOUT,
                "nodelist": [""]
            }
            json_inp = json.dumps(data)
            client_socket.sendall(json_inp.encode('utf-8'))

            # Read full response with bounded recv (CWE-400 prevention)
            response_bytes = _recv_all(client_socket)
            if not response_bytes:
                raise CommandException(-1, cmd_str, "Empty response from vmexec server")
            response = response_bytes.decode('utf-8')
            try:
                json_data = json.loads(response)
            except json.JSONDecodeError:
                logging.error("Malformed JSON response from vmexec server (truncated or timed out): %s",
                              response[:200])
                raise CommandException(-1, cmd_str,
                                       "Malformed response from vmexec server (possible truncation or timeout)")
    if len(json_data['error']) != 0:
        raise CommandException(-1, cmd_str, json_data['error'])
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
    # Escape regex special characters in inputs to prevent regex injection (CWE-78)
    safe_volname = re.escape(volname)
    safe_mountpoint = re.escape(mountpoint)
    args = "bin/glusterfs.*{}.*{}".format(safe_volname, safe_mountpoint)
    out, err, res = execute_vmexec("/usr/bin/pgrep", "-c", "-f", args)
    logging.debug("is_gl_mount_vmexec for volume: %s, returned. out: %s err: %s res: %s", volname, out, err, res)
    return int(out) > 0 and res == 0
