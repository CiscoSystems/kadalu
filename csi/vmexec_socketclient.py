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
    # Enhanced resilience: exponential backoff with jitter for slow/unstable connections
    base_retry_interval = 2.0
    max_retry_interval = 60
    max_retries = 15
    socket_timeout = 30
    jitter_factor = 0.1

    retry_count = 0
    connected = False
    while retry_count < max_retries and not connected:
        try:
            # Set socket timeout to handle slow responses
            client_socket.settimeout(socket_timeout)
            # Attempt to connect to the server
            client_socket.connect(SOCKET_FILE_PATH)
            connected = True
            logging.debug("Socket connected to the vmexec!")
            break
        except (ConnectionRefusedError, OSError, socket.timeout) as e:
            # Enhanced error handling for various connection issues
            retry_count += 1
            
            # Calculate exponential backoff with jitter
            backoff_time = min(base_retry_interval * (2 ** (retry_count - 1)), max_retry_interval)
            # Fixed jitter calculation to avoid potential division issues and provide proper randomization
            import random
            jitter = backoff_time * jitter_factor * (random.random() - 0.5) * 2  # ±jitter_factor * backoff_time
            sleep_time = max(0.1, backoff_time + jitter)  # Ensure minimum 0.1s delay
            
            error_type = type(e).__name__
            logging.warning(f"Socket connection failed (attempt {retry_count}/{max_retries}): {error_type} - {str(e)}. "
                          f"Retrying in {sleep_time:.2f} seconds...")
            
            if retry_count < max_retries:
                time.sleep(sleep_time)
        except Exception as e:
            # Handle unexpected errors
            retry_count += 1
            logging.error(f"Unexpected socket error (attempt {retry_count}/{max_retries}): {type(e).__name__} - {str(e)}")
            if retry_count < max_retries:
                time.sleep(min(base_retry_interval * retry_count, max_retry_interval))

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
    
    # Enhanced socket client with connection pooling and resilience
    max_connection_attempts = 3
    connection_attempt = 0
    
    while connection_attempt < max_connection_attempts:
        connection_attempt += 1
        
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
                # Set socket options for better resilience
                client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                
                is_connected = connect_socket(client_socket)
                if not is_connected:
                    error_msg = f"Unable to connect to the vmexec server after max retries (attempt {connection_attempt}/{max_connection_attempts})."
                    logging.error(error_msg)
                    
                    if connection_attempt < max_connection_attempts:
                        logging.info(f"Retrying socket client connection in 2 seconds...")
                        time.sleep(2)
                        continue
                    else:
                        json_data["error"] = "Unable to connect to the server for command execution after all attempts."
                        return json_data['output'], json_data['error'], int(json_data['result'])
                else:
                    logging.debug("Sending the command to socket server for execution")

                    data = {
                        "id": str(uuid.uuid4()),
                        "commandtype": 4,
                        "command": cmd,
                        "hidden": False,
                        "commandtimeout": 120,  # Increased timeout for slow operations
                        "nodelist": [""]
                    }
                    json_inp = json.dumps(data)
                    
                    try:
                        # Send with timeout handling
                        client_socket.settimeout(10)  # 10 second send timeout
                        client_socket.sendall(json_inp.encode('utf-8'))

                        # Enhanced receive with buffer handling for large responses
                        client_socket.settimeout(130)  # Slightly more than command timeout
                        response_parts = []
                        bytes_received = 0
                        max_response_size = 1024 * 1024  # 1MB max response
                        
                        while bytes_received < max_response_size:
                            try:
                                chunk = client_socket.recv(4096)
                                if not chunk:
                                    break
                                response_parts.append(chunk)
                                bytes_received += len(chunk)
                                
                                # Try to parse as complete JSON
                                try:
                                    full_response = b''.join(response_parts).decode('utf-8')
                                    json_data = json.loads(full_response)
                                    break
                                except (json.JSONDecodeError, UnicodeDecodeError):
                                    # Continue receiving if JSON is incomplete
                                    continue
                            except socket.timeout:
                                logging.warning(f"Socket receive timeout after {bytes_received} bytes")
                                break
                        
                        if not response_parts:
                            raise socket.timeout("No response received from server")
                            
                    except (socket.timeout, socket.error) as e:
                        error_msg = f"Socket communication error: {type(e).__name__} - {str(e)}"
                        logging.error(error_msg)
                        
                        if connection_attempt < max_connection_attempts:
                            logging.info(f"Retrying socket communication (attempt {connection_attempt + 1}/{max_connection_attempts})...")
                            time.sleep(1)
                            continue
                        else:
                            json_data["error"] = f"Socket communication failed: {error_msg}"
                            return json_data['output'], json_data['error'], int(json_data['result'])
                    
                    # Successfully received response, break out of retry loop
                    break
                    
        except Exception as e:
            error_msg = f"Unexpected socket client error: {type(e).__name__} - {str(e)}"
            logging.error(error_msg)
            
            if connection_attempt < max_connection_attempts:
                logging.info(f"Retrying due to unexpected error (attempt {connection_attempt + 1}/{max_connection_attempts})...")
                time.sleep(2)
                continue
            else:
                json_data["error"] = error_msg
                return json_data['output'], json_data['error'], int(json_data['result'])
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
