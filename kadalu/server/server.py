#!/usr/bin/python3

"""
Prepares, Validates and then starts the Moana Agent Process
"""

import os

from kadalu.server.storage_unit_utils import (
    create_and_mount_storage_unit,
    create_storage_unit_dir,
    verify_storage_unit_dir_xattr_support
)
from kadalu.common.utils import Monitor, Proc, logging_setup


def start_server_process():
    """
    Start Moana Agent Process and Exporter Service
    """
    logging_setup()

    storage_unit_device = os.environ.get("STORAGE_UNIT_DEVICE", None)
    storage_unit_path = os.environ["STORAGE_UNIT_PATH"]
    if storage_unit_device is not None and storage_unit_device != "":
        storage_unit_fs = os.environ.get("STORAGE_UNIT_FS", "xfs")
        create_and_mount_storage_unit(storage_unit_device, storage_unit_path,
                                      storage_unit_fs)

    create_storage_unit_dir(storage_unit_path)
    verify_storage_unit_dir_xattr_support(storage_unit_path)

    mon = Monitor()

    curr_dir = os.path.dirname(__file__)
    mon.add_process(Proc("metrics", "python3", [curr_dir + "/exporter.py"]))
    mon.add_process(Proc("Storage Manager", "kadalu", ["mgr"]))

    mon.start_all()
    mon.monitor()


if __name__ == "__main__":
    start_server_process()