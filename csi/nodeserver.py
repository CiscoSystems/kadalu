"""
nodeserver implementation
"""
import logging
import os
import time

import csi_pb2
import csi_pb2_grpc
import grpc
from kadalulib import logf, is_gluster_mount_proc_running
from volumeutils import mount_glusterfs, mount_volume, unmount_volume, verify_mount

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

     mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)

     pvpath_full = os.path.join(mntdir, pvpath)

     logging.debug(logf(
        "Received a valid mount request",
        request=request,
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

     # Enhanced retry logic with exponential backoff for slow disk scenarios
     max_retries = 8  # Increased for slow storage
     base_retry_interval = 3  # seconds
     max_retry_interval = 60  # seconds
     retry_count = 0
     mounted_successfully = False

     while retry_count < max_retries and not mounted_successfully:
        try:
            # Calculate exponential backoff for mount operations
            retry_delay = min(base_retry_interval * (2 ** retry_count), max_retry_interval)
            
            if retry_count > 0:
                logging.info(logf(
                    "Retrying mount operation with exponential backoff",
                    attempt=retry_count + 1,
                    max_attempts=max_retries,
                    delay_seconds=retry_delay,
                    mountpoint=mntdir
                ))
                time.sleep(retry_delay)
            
            mount_glusterfs(volume, mntdir, True)
            
            # Enhanced mount verification with I/O error recovery
            mount_verification_timeout = 45 + (retry_count * 15)  # Increase timeout for slow disks
            verification_start = time.time()
            
            # First verification attempt
            mount_verified = verify_mount(mntdir, timeout=mount_verification_timeout)
            
            if mount_verified:
                mounted_successfully = True
                logging.info(logf("Mount verified and successful", 
                                mountpoint=mntdir, 
                                verification_time=time.time() - verification_start))
                break
            else:
                # Check if verification failed due to I/O errors vs. actual mount failure
                # Wait a bit for filesystem to stabilize after mount
                stabilization_wait = min(10 + (retry_count * 5), 30)
                logging.info(logf(
                    "Initial mount verification failed, waiting for filesystem stabilization",
                    mountpoint=mntdir,
                    wait_seconds=stabilization_wait,
                    attempt=retry_count + 1
                ))
                time.sleep(stabilization_wait)
                
                # Retry verification with extended timeout for slow I/O recovery
                extended_timeout = mount_verification_timeout + 30
                mount_verified = verify_mount(mntdir, timeout=extended_timeout)
                
                if mount_verified:
                    mounted_successfully = True
                    total_verification_time = time.time() - verification_start
                    logging.info(logf(
                        "Mount verified successful after stabilization wait", 
                        mountpoint=mntdir, 
                        total_verification_time=total_verification_time,
                        stabilization_wait=stabilization_wait
                    ))
                    break
                else:
                    # Final check: verify the GlusterFS process is actually running
                    # This helps distinguish between I/O errors and mount failures
                    try:
                        if is_gluster_mount_proc_running(volume["name"], mntdir):
                            logging.warning(logf(
                                "GlusterFS process running but verification failed - possible I/O issues",
                                mountpoint=mntdir,
                                volume_name=volume["name"],
                                attempt=retry_count + 1
                            ))
                            # Consider this a temporary I/O issue, not a mount failure
                            # Continue to retry logic with appropriate error categorization
                            raise Exception(f"Mount process running but I/O verification failed after {total_verification_time:.2f}s - possible network/storage issues.")
                        else:
                            raise Exception(f"Mount verification failed after {total_verification_time:.2f}s - mount process not running.")
                    except Exception as proc_check_error:
                        raise Exception(f"Mount verification failed after {total_verification_time:.2f}s - {str(proc_check_error)}")
                
        except Exception as e:
            retry_count += 1
            error_msg = str(e)
            
            # Enhanced error categorization for better handling
            is_timeout_error = "timeout" in error_msg.lower() or "time out" in error_msg.lower()
            is_transport_error = any(term in error_msg.lower() for term in [
                "transport endpoint", "connection refused", "no route to host", 
                "network unreachable", "connection reset"
            ])
            is_io_error = any(term in error_msg.lower() for term in [
                "input/output error", "i/o error", "device or resource busy",
                "read-only file system", "no space left"
            ])
            
            error_category = "general"
            if is_timeout_error:
                error_category = "timeout"
            elif is_transport_error:
                error_category = "transport"
            elif is_io_error:
                error_category = "io"
            
            if retry_count < max_retries:
                # Adjust retry delay based on error type
                if error_category == "transport":
                    retry_delay = min(base_retry_interval * (2 ** retry_count), max_retry_interval)
                elif error_category == "timeout" or error_category == "io":
                    retry_delay = min(base_retry_interval * (3 ** retry_count), max_retry_interval * 2)
                else:
                    retry_delay = min(base_retry_interval * (2 ** retry_count), max_retry_interval)
                    
                logging.warning(logf(
                    "Mount attempt failed, will retry with enhanced backoff",
                    attempt=retry_count,
                    max_attempts=max_retries,
                    mountpoint=mntdir,
                    error=error_msg,
                    error_category=error_category,
                    next_retry_delay=retry_delay
                ))
            else:
                logging.error(logf(
                    "All mount retry attempts exhausted",
                    final_attempt=retry_count,
                    max_attempts=max_retries,
                    mountpoint=mntdir,
                    error=error_msg,
                    error_category=error_category
                ))

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
