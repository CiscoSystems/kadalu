"""
Starting point of CSI driver GRP server
"""
import logging
import os
import sys
import time
import threading
from concurrent import futures
from http.server import HTTPServer, BaseHTTPRequestHandler

import csi_pb2_grpc
import grpc
from controllerserver import ControllerServer
from identityserver import IdentityServer
from kadalulib import CommandException, logf, logging_setup
from nodeserver import NodeServer
from volumeutils import (HOSTVOL_MOUNTDIR, get_pv_hosting_volumes,
                         mount_glusterfs)

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
HEALTH_PORT = 9808

# Reference to the gRPC server, set in main() for health checks
_grpc_server = None


class HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for liveness probes."""

    def do_GET(self):  # noqa: N802
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):  # noqa: A002
        # Suppress per-request log spam from kubelet probes
        pass


def start_health_server():
    """Start the liveness health HTTP server on a daemon thread."""
    srv = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    logging.info(logf("Health server started", port=HEALTH_PORT))


def mount_storage():
    """
    Mount storage if any volumes exist after a pod reboot
    """
    if os.environ.get("CSI_ROLE", "-") != "provisioner":
        logging.debug("Volume need to be mounted on only provisioner pod")
        return

    host_volumes = get_pv_hosting_volumes({})
    for volume in host_volumes:
        if volume["single_pv_per_pool"]:
            # Need to skip mounting external non-native mounts in-order for
            # kadalu-quotad not to set quota xattrs
            continue
        hvol = volume["name"]
        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hvol)
        try:
            mount_glusterfs(volume, mntdir)
            logging.info(logf("Volume is mounted successfully", hvol=hvol))
        except CommandException:
            logging.error(logf("Unable to mount volume", hvol=hvol))
    return


def main():
    """
    Register Controller Server, Node server and Identity Server and start
    the GRPC server in required endpoint
    """
    logging_setup()

    # Start health endpoint for kubelet liveness probes
    if os.environ.get("CSI_ROLE", "-") == "provisioner":
        start_health_server()

    # If Provisioner pod reboots, mount volumes if they exist before reboot
    mount_storage()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    csi_pb2_grpc.add_ControllerServicer_to_server(ControllerServer(), server)
    csi_pb2_grpc.add_NodeServicer_to_server(NodeServer(), server)
    csi_pb2_grpc.add_IdentityServicer_to_server(IdentityServer(), server)

    # Validate CSI_ENDPOINT is a Unix domain socket (CWE-319 prevention)
    csi_endpoint = os.environ.get("CSI_ENDPOINT", "unix://plugin/csi.sock")
    if not csi_endpoint.startswith("unix://"):
        logging.error("CSI_ENDPOINT must use unix:// scheme, refusing to start with: %s",
                      csi_endpoint.split("://")[0] + "://...")
        sys.exit(1)

    server.add_insecure_port(csi_endpoint)
    logging.info("Server started")
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    main()
