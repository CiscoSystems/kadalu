#!/bin/bash
#
# sync-gluster-hosts.sh
#
# Runs inside the kadalu CSI nodeplugin pod (a DaemonSet, so one copy on every
# node). Its job is to keep the host's /etc/hosts in sync with the gluster
# IP->hostname mapping published by the orchestrator in the `kadalu-gluster-hosts`
# ConfigMap.
#
# Why this exists:
#   On IPv6-only clusters the gluster volfile servers are addressed by
#   deterministic hostnames (gluster-node-1..N) instead of bare IPv6 literals,
#   because the bundled glusterfs 11.2 resolver truncates bare IPv6 literals at
#   the last colon. The actual fuse mount is executed on the HOST (via the
#   vmexec service), using the HOST's /usr/sbin/glusterfs and the HOST's
#   /etc/hosts. Master/Hybrid nodes get the gluster-node-<N> mapping written to
#   /etc/hosts by the orchestrator's setup_gluster_hostnames(), but Worker nodes
#   never run that code, so without this reconciler a mount scheduled on a worker
#   cannot resolve gluster-node-<N> and fails.
#
# This script reconciles a managed block in the host /etc/hosts (mounted into the
# pod at HOST_HOSTS) from the ConfigMap (mounted at HOSTMAP_FILE). It is
# idempotent: it only rewrites the file when the desired managed block differs
# from what is already present. It never touches lines outside its own
# BEGIN/END markers. IPv4 deployments simply never publish the ConfigMap, so the
# script stays a no-op there.

set -u

# Host /etc/hosts, bind-mounted from the node via hostPath.
HOST_HOSTS="${HOST_HOSTS:-/host/etc/hosts}"
# ConfigMap projection: the `hosts` key holds "<ip> gluster-node-<N>" lines.
HOSTMAP_FILE="${HOSTMAP_FILE:-/var/lib/kadalu-hostmap/hosts}"

BEGIN_MARKER="# BEGIN crosswork-gluster-hosts"
END_MARKER="# END   crosswork-gluster-hosts"

# How often to reconcile (seconds). ConfigMap projections are updated by the
# kubelet asynchronously, so a periodic poll is simpler and more robust than
# watching for inotify events on the symlink-swapped projection directory.
POLL_INTERVAL="${GLUSTER_HOSTS_SYNC_INTERVAL:-15}"

log() {
    echo "[sync-gluster-hosts] $*"
}

# Build the desired managed block (markers + mapping lines) on stdout. Returns
# non-zero (and prints nothing) when there is no mapping to apply.
build_desired_block() {
    [ -s "${HOSTMAP_FILE}" ] || return 1
    # Keep only well-formed "<token> <token>" lines (defensive against a
    # malformed ConfigMap); drop comments and blanks.
    local body
    body=$(awk 'NF >= 2 && $1 !~ /^#/ {print $1, $2}' "${HOSTMAP_FILE}")
    [ -n "${body}" ] || return 1
    printf '%s\n%s\n%s\n' "${BEGIN_MARKER}" "${body}" "${END_MARKER}"
}

# Return the current managed block from the host /etc/hosts (markers included),
# or empty if absent.
current_block() {
    [ -f "${HOST_HOSTS}" ] || return 0
    awk -v b="${BEGIN_MARKER}" -v e="${END_MARKER}" '
        $0 == b {p=1}
        p {print}
        $0 == e {p=0}
    ' "${HOST_HOSTS}"
}

reconcile() {
    local desired
    desired=$(build_desired_block) || return 0

    if [ ! -f "${HOST_HOSTS}" ]; then
        log "host hosts file ${HOST_HOSTS} not present, skipping"
        return 0
    fi

    local current
    current=$(current_block)
    if [ "${current}" = "${desired}" ]; then
        return 0
    fi

    # Rewrite: strip any existing managed block, then append the desired one.
    # We edit the file in place (the directory is the host's /etc, which we do
    # not own, so an atomic rename into it is not available; an in-place
    # truncate+write of a single file mounted via hostPath is the supported
    # pattern here).
    local stripped
    stripped=$(awk -v b="${BEGIN_MARKER}" -v e="${END_MARKER}" '
        $0 == b {skip=1; next}
        $0 == e {skip=0; next}
        !skip {print}
    ' "${HOST_HOSTS}")

    {
        printf '%s\n' "${stripped}"
        printf '%s\n' "${desired}"
    } > "${HOST_HOSTS}"

    log "updated managed gluster-node mapping in ${HOST_HOSTS}"
}

log "starting; host=${HOST_HOSTS} map=${HOSTMAP_FILE} interval=${POLL_INTERVAL}s"
while true; do
    reconcile
    sleep "${POLL_INTERVAL}"
done
