"""
nodeserver implementation
"""
import logging
import os
import time
import threading
import csi_pb2
import csi_pb2_grpc
import grpc
from kadalulib import logf
from volumeutils import mount_glusterfs, mount_volume, unmount_volume

HOSTVOL_MOUNTDIR = "/mnt/cw_glusterfs/kadalu"
GLUSTERFS_CMD = "/opt/sbin/glusterfs"
MOUNT_CMD = "/bin/mount"
UNMOUNT_CMD = "/bin/umount"

# noqa # pylint: disable=too-many-locals
# noqa # pylint: disable=too-many-statements

class NodeServer(csi_pb2_grpc.NodeServicer):
    """
    NodeServer object is responsible for handling host
    volume mount and PV mounts.
    Ref:https://github.com/container-storage-interface/spec/blob/master/spec.md
    """
    mount_lock = threading.Lock()
    def NodePublishVolume(self, request, context):
        start_time = time.time()
    errmsg = ""

    # Perform request validation
    required_fields = {
        'volume_id': request.volume_id,
        'target_path': request.target_path,
        'volume_capability': request.volume_capability,
        'volume_context': request.volume_context
    }
    for field_name, field_value in required_fields.items():
        if not field_value:
            errmsg = f"{field_name.replace('_', ' ').capitalize()} is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()

    # Extract volume information from the request
    volume_context = request.volume_context
    hostvol = volume_context.get("hostvol", "")
    pvpath = volume_context.get("path", "")
    voltype = volume_context.get("type", "")
    gserver = volume_context.get("gserver", None)
    gvolname = volume_context.get("gvolname", None)
    options = volume_context.get("options", None)

    mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)
    pvpath_full = os.path.join(mntdir, pvpath)

    logging.debug(logf(
        "Received a valid mount request",
        request=request,
        voltype=voltype,
        hostvol=hostvol,
        pvpath=pvpath,
        pvpath_full=pvpath_full
    ))

    # Synchronize mounting operations to prevent race conditions
    with mount_lock:
        # Check if the target path is already mounted
        if not os.path.ismount(request.target_path):
            # Mount the hosting volume if not already mounted
            mount_glusterfs(volume, mntdir, True)


            if voltype == "External":
                logging.debug(logf(
                    "Mounted Volume for PV",
                    volume=volume,
                    mntdir=mntdir
                ))

            # Mount the PV to the target path
            if not mount_volume(pvpath_full, request.target_path, voltype, fstype=None):
                errmsg = "Unable to bind PV to target path"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.NodePublishVolumeResponse()

    # Log the successful mount
    logging.info(logf(
        "Mounted PV",
        volume=request.volume_id,
        pvpath=pvpath,
        hostvol=hostvol,
        target_path=request.target_path,
        duration_seconds=time.time() - start_time
    ))

    return csi_pb2.NodePublishVolumeResponse()

    def NodeUnpublishVolume(self, request, context):
        # TODO: Validation and handle target_path failures

        if not request.volume_id:
            errmsg = "Volume ID is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodeUnpublishVolumeResponse()

        if not request.target_path:
            errmsg = "Target path is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodeUnpublishVolumeResponse()

        logging.debug(logf(
            "Received the unmount request",
            volume=request.volume_id,
        ))
        unmount_volume(request.target_path)

        return csi_pb2.NodeUnpublishVolumeResponse()

    def NodeGetCapabilities(self, request, context):
        return csi_pb2.NodeGetCapabilitiesResponse()

    def NodeGetInfo(self, request, context):
        return csi_pb2.NodeGetInfoResponse(
            node_id=os.environ["NODE_ID"],
        )

    def NodeExpandVolume(self, request, context):

        logging.warning(logf(
            "NodeExpandVolume called, which is not implemented."
        ))

        return csi_pb2.NodeExpandVolumeResponse()
