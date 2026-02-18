"""
nodeserver implementation
"""
import logging
import os
import re
import time

import csi_pb2
import csi_pb2_grpc
import grpc
from kadalulib import logf
from volumeutils import mount_glusterfs, mount_volume, unmount_volume, verify_mount

HOSTVOL_MOUNTDIR = "/mnt/cw_glusterfs/kadalu"
GLUSTERFS_CMD = "/opt/sbin/glusterfs"
MOUNT_CMD = "/bin/mount"
UNMOUNT_CMD = "/bin/umount"

# Kubernetes-safe name pattern: lowercase alphanumeric, dashes, dots (CWE-22 prevention)
_SAFE_NAME_PATTERN = re.compile(r'^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$')


def _validate_k8s_name(name, field_name):
    """Validate that a name conforms to Kubernetes naming conventions."""
    if not name or len(name) > 253:
        raise ValueError("Invalid %s: empty or exceeds 253 chars" % field_name)
    if not _SAFE_NAME_PATTERN.match(name):
        raise ValueError("Invalid %s: contains unsafe characters: %s" % (field_name, repr(name)))
    return name


def _validate_path_within(child_path, parent_path):
    """Ensure child_path resolves within parent_path (CWE-22 path traversal prevention)."""
    # Use realpath to resolve symlinks and ../ traversal
    real_parent = os.path.realpath(parent_path)
    real_child = os.path.realpath(child_path)
    if not real_child.startswith(real_parent + os.sep) and real_child != real_parent:
        raise ValueError("Path traversal detected: %s is not under %s" % (child_path, parent_path))
    return real_child

# noqa # pylint: disable=too-many-locals
# noqa # pylint: disable=too-many-statements

class NodeServer(csi_pb2_grpc.NodeServicer):
    """
    NodeServer object is responsible for handling host
    volume mount and PV mounts.
    Ref:https://github.com/container-storage-interface/spec/blob/master/spec.md
    """
    def NodePublishVolume(self, request, context):
     start_time = time.time()
     if not request.volume_id:
        errmsg = "Volume ID is empty and must be provided"
        logging.error(errmsg)
        context.set_details(errmsg)
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        return csi_pb2.NodePublishVolumeResponse()

     if not request.target_path:
        errmsg = "Target path is empty and must be provided"
        logging.error(errmsg)
        context.set_details(errmsg)
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        return csi_pb2.NodePublishVolumeResponse()

     if not request.volume_capability:
        errmsg = "Volume capability is empty and must be provided"
        logging.error(errmsg)
        context.set_details(errmsg)
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        return csi_pb2.NodePublishVolumeResponse()

     if not request.volume_context:
        errmsg = "Volume context is empty and must be provided"
        logging.error(errmsg)
        context.set_details(errmsg)
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        return csi_pb2.NodePublishVolumeResponse()

     hostvol = request.volume_context.get("hostvol", "")
     pvpath = request.volume_context.get("path", "")
     pvtype = request.volume_context.get("pvtype", "")
     voltype = request.volume_context.get("type", "")
     gserver = request.volume_context.get("gserver", None)
     gvolname = request.volume_context.get("gvolname", None)
     options = request.volume_context.get("options", None)

     # Validate volume name to prevent path traversal (CWE-22)
     try:
        _validate_k8s_name(hostvol, "hostvol")
     except ValueError as err:
        errmsg = "Invalid hostvol name: %s" % str(err)
        logging.error(errmsg)
        context.set_details(errmsg)
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        return csi_pb2.NodePublishVolumeResponse()

     mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)

     pvpath_full = os.path.join(mntdir, pvpath)

     # Validate pvpath does not escape the mount directory (CWE-22)
     try:
        _validate_path_within(pvpath_full, mntdir)
     except ValueError as err:
        errmsg = "Path traversal rejected: %s" % str(err)
        logging.error(errmsg)
        context.set_details(errmsg)
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        return csi_pb2.NodePublishVolumeResponse()

     logging.debug(logf(
        "Received a valid mount request",
        volume_id=request.volume_id,
        voltype=voltype,
        hostvol=hostvol,
        pvpath=pvpath,
        pvtype=pvtype,
        pvpath_full=pvpath_full
    ))

     volume = {
        'name': hostvol,
        'g_volname': gvolname,
        'g_host': gserver,
        'g_options': options,
        'type': voltype,
    }

     max_retries = 5
     retry_interval = 5  # seconds
     retry_count = 0
     mounted_successfully = False

     while retry_count < max_retries and not mounted_successfully:
        try:
            mount_glusterfs(volume, mntdir, True)
            if verify_mount(mntdir):
                    mounted_successfully = True
                    logging.info(logf("Mount verified and successful", mountpoint=mntdir))
            else:
                  # Unmount the failed mount before retrying to prevent
                  # orphaned FUSE processes from accumulating (mount leak).
                  try:
                      from volumeutils import unmount_glusterfs
                      unmount_glusterfs(mntdir)
                  except Exception as unmount_err:
                      logging.warning(logf(
                          "Failed to unmount before retry",
                          mountpoint=mntdir,
                          error=str(unmount_err)
                      ))
                  raise OSError("Mount verification failed.")
        except (OSError, IOError, ConnectionError) as e:
            retry_count += 1
            logging.warning(logf(
                "Retrying mount due to failure",
                attempt=retry_count,
                max_attempts=max_retries,
                mountpoint=mntdir,
                error=str(e)
            ))
            time.sleep(retry_interval)
        except (ValueError, PermissionError) as e:
            # Non-retriable errors: invalid args, permission denied
            errmsg = "Mount failed with non-retriable error: %s" % str(e)
            logging.error(logf(errmsg, mountpoint=mntdir))
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INTERNAL)
            return csi_pb2.NodePublishVolumeResponse()

     if not mounted_successfully:
        errmsg = f"All {max_retries} retry attempts to mount volume failed."
        logging.error(logf(errmsg, mountpoint=mntdir))
        context.set_details(errmsg)
        context.set_code(grpc.StatusCode.INTERNAL)
        return csi_pb2.NodePublishVolumeResponse()

     if voltype == "External":
        logging.debug(logf(
            "Mounted Volume for PV",
            volume=volume,
            mntdir=mntdir
        ))
        # return csi_pb2.NodePublishVolumeResponse()

     logging.debug(logf(
        "Mounted Hosting Volume",
        pv=request.volume_id,
        hostvol=hostvol,
        mntdir=mntdir
    ))
    # Mount the PV
    # TODO: Handle Volume capability mount flags
     if mount_volume(pvpath_full, request.target_path, pvtype, fstype=None):
        logging.info(logf(
            "Mounted PV",
            volume=request.volume_id,
            pvpath=pvpath,
            pvtype=pvtype,
            hostvol=hostvol,
            target_path=request.target_path,
            duration_seconds=time.time() - start_time
        ))
     else:
        errmsg = "Unable to bind PV to target path"
        logging.error(errmsg)
        context.set_details(errmsg)
        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
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
