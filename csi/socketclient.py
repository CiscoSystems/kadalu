import os
import json
import socket
import uuid
import logging

from kadalulib import (execute, CommandException)

SOCKET_FILE_PATH = "/var/run/vmexec-socket/vmexec.sock"
GLUSTERFS_CMD = "/usr/sbin/glusterfs"
MOUNT_CMD = "/usr/bin/mount"
UNMOUNT_CMD = "/usr/bin/umount"
HOSTVOL_MOUNTDIR = "/mnt/kadalu"

cmdList = ["glusterfs", "/mount", "/umount", "findmnt", "losetup"]

def connectSocket(client_socket):
    retry_interval = 60
    max_retries = 5

    retry_count = 0
    connected = False
    while retry_count < max_retries and not connected:
        try:
            # Attempt to connect to the server
            client_socket.connect(SOCKET_FILE_PATH)
            connected = True
            logging.debug("Connected to the server!")
            break
        except ConnectionRefusedError:
            # Connection refused, wait for a while before retrying
            logging.debug("Connection refused. Retrying in seconds..." + str(retry_interval))
            time.sleep(retry_interval)
            retry_count += 1

    return connected

def changeLogLevel(commandList):
    for i in range(len(commandList)):
        if "log-level" in commandList[i] and "DEBUG" in commandList[i][-5:]:
            commandList[i] = commandList[i][:-5] + "INFO"
    return commandList


def parseCommand(commandList):
    if "glusterfs" in commandList[0]:
        commandList[0] = GLUSTERFS_CMD
    elif "/mount" in commandList[0]:
        commandList[0] = MOUNT_CMD
    elif "/umount" in commandList[0]:
        commandList[0] = UNMOUNT_CMD

    commandList = changeLogLevel(commandList)
    return " ".join(commandList)

def socketClient(commandList):
    cmd = parseCommand(commandList)
    client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    json_data = json.dumps({
        "error" : "Unable to execute command",
        "result": 1,
        "output" : ""
    })

    logging.debug("Connecting socket")
    is_connected = connectSocket(client_socket)

    if not is_connected:
        logging.error("Unable to connect to the server after max retries.")
        json_data["error"] = "Unable to connect to the server for command execution after max retries."
    else:
        try:
            logging.info("Seding the command to socket server for execution")

            data = {
                "id" : str(uuid.uuid4()),
                "commandtype" : 4,
                "command" : cmd,
                "hidden" : False,
                "commandtimeout" : 100,
                "nodelist" : [""]
            }
            json_inp = json.dumps(data)
            client_socket.sendall(json_inp.encode('utf-8'))

            #receive a response from the server
            response = client_socket.recv(1024).decode('utf-8')
            json_data = json.loads(response)
        finally:
            # Close the connection
            client_socket.close()
    if len(json_data['error']) != 0 :
                raise CommandException(json_data['result'], json_data['output'], json_data['error'])
    return (json_data["output"], json_data["error"], json_data['result'])

def executeCommand(*cmd):
    head = cmd[0]
    logging.info(cmd)
    if any(cmdStr in head for cmdStr in cmdList):
        logging.info("The command is glusterfs or mount or umount or findmnt")
        return socketClient(list(cmd))
    else:
        return execute(*cmd)
