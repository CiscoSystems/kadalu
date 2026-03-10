# Kadalu CSI Provisioner — Showtech Root Cause Analysis
## Showtech: `showtech_all_20260309093233` | Node: `172.22.235.73`

---

## Executive Summary

**ALL 33 GlusterFS External volume FUSE mounts fail on the provisioner pod** due to a
**container-vs-host mount path mismatch** when `CREATE_MOUNT_ON_VMEXEC=True`. The provisioner
sends mount commands with container-local paths (`/mnt/<vol>`) to vmexec, which runs them on
the host where the actual path is `/mnt/cw_glusterfs/kadalu/<vol>`. This triggers an infinite
pgrep→mount→fail loop that has been running for **2h 47min**, generating **100,000+ failed
commands** through vmexec, consuming **2 CPU cores at 90%+ each**, and blocking **126 PVCs**
in Pending state on the cluster. Despite the mount failures, 20 PVCs were eventually created
(taking **~120 seconds each** instead of < 1 second) because the old buggy code returns the
mountpoint on failure, writing PVC metadata to the unmounted local path.

---

## Root Cause: Container-vs-Host Path Mismatch

### The Bug

| Component | `HOSTVOL_MOUNTDIR` | Mount path used | Example |
|---|---|---|---|
| **Provisioner pod** (container view) | `/mnt` | `/mnt/storage-vol1` | `glusterfs ... /mnt/storage-vol1` |
| **Node plugin pod** (container view) | `/mnt/cw_glusterfs/kadalu` | `/mnt/cw_glusterfs/kadalu/storage-vol1` | `glusterfs ... /mnt/cw_glusterfs/kadalu/storage-vol1` |
| **Host** (via vmexec) | N/A | `/mnt/cw_glusterfs/kadalu/storage-vol1` | Actual FUSE processes run here |

The provisioner container's `/mnt` is mapped (via k8s hostPath volume mount) to the host's
`/mnt/cw_glusterfs/kadalu/`. This means:

1. **Python `makedirs("/mnt/storage-vol1")`** inside the container → creates
   `Host:/mnt/cw_glusterfs/kadalu/storage-vol1` (correct, via hostPath)
2. **`execute_vmexec("glusterfs", ..., "/mnt/storage-vol1")`** → sends literal path
   `/mnt/storage-vol1` to vmexec → vmexec runs on the HOST → `/mnt/storage-vol1` **does not
   exist** on host → `exit status 1`
3. **`pgrep -c -f "bin/glusterfs.*glustervol1.*/mnt/storage\-vol1"`** → searches HOST
   processes → actual processes have `/mnt/cw_glusterfs/kadalu/storage-vol1` → **no match** →
   returns 0

The vmexec socket client bypasses the container's mount namespace, creating a path translation
gap that makes every mount and pgrep call fail.

---

## Quantitative Evidence from Showtech

### Provisioner Container Log (11,169 lines, 4.7-minute window)
| Metric | Value |
|---|---|
| Mount command failures | **1,580** |
| Unique volumes failing | **ALL 33** |
| CreateVolume succeeded (eventually) | **20** |
| Average CreateVolume duration | **120.2 seconds** (should be < 1s) |
| RESOURCE_EXHAUSTED errors | **0** |
| Time window | 09:42:42 → 09:47:22 |

### Top Failing Volumes (by failure count)
| Volume | Failures |
|---|---|
| glustervol1, glustervol2 | 72 each |
| cw_localfs | 68 |
| rssysagg, rsshtch, rscmmndf, cw_dregfs | 60 each |
| glustervol3 | 50 |

### vmexec Journal (126,522 lines, 2h 47min window)
| Metric | Value |
|---|---|
| Total exit status 1 errors | **59,821** |
| pgrep commands from provisioner | **40,477** |
| glusterfs mount commands from provisioner | **60,476** |
| Total commands to vmexec | **~100,953** |
| Storm duration | 06:59:51 → 09:46:57 (**2h 47min continuous**) |
| pgrep commands returning success | **0** (never matched) |

### Node Plugin (WORKS FINE — 3,340 lines)
| Metric | Value |
|---|---|
| NodePublishVolume successes | **1,240+** |
| Average mount duration | **0.33 seconds** |
| Mount path used | `/mnt/cw_glusterfs/kadalu/<vol>` |
| Failed mounts | **0** |

### System Health at Showtech Time
| Metric | Value | Status |
|---|---|---|
| Load average | **12.77** | CRITICAL |
| Total tasks | **2,157** | HIGH |
| RAM used / total | 47.4 GB / 125.7 GB | OK |
| 2x pgrep processes | **90.5% CPU each** | CRITICAL (from provisioner) |
| vmexec tasks | **882** | HIGH |
| vmexec memory (peak) | 874.7 MB (**3.1 GB** peak) | HIGH |
| vmexec CPU time | 4h 15min | HIGH |
| Total mounts on system | **1,038** | HIGH |

### GlusterFS Server (ALL HEALTHY)
| Metric | Value |
|---|---|
| glusterd status | active (running) since 06:54:39 |
| Volumes | 30 total, ALL Started |
| Bricks | ALL Online |
| Peers | 0 (standalone) |
| Disk free (glusterfs LVM) | 186.3 GB / 235.9 GB (22%) |
| Disk free (dregfs LVM) | 39.2 GB / 49.9 GB (22%) |
| Disk free (localfs LVM) | 8.8 GB / 9.4 GB (3%) |
| Inodes free | 123M+ |
| Clients connected per vol | 1 (node plugin's mounts) |

### Kubernetes Cluster
| Metric | Value |
|---|---|
| PVCs in Pending state | **126** |
| PVCs in Bound state | 710 |
| kubectl-events.log | **0 lines** (empty) |

---

## Failure Timeline

```
06:54:39  glusterd starts on host (after reboot/restart)
06:59:49  vmexec service starts (PID 19055)
06:59:50  vmexec listening on /var/run/vmexec-socket/vmexec.sock
06:59:50  PROVISIONER starts sending pgrep checks → ALL FAIL (path mismatch)
06:59:51  PROVISIONER starts sending mount commands to /mnt/<vol> → ALL FAIL (path doesn't exist on host)
06:59:52  NODE PLUGIN mounts at /mnt/cw_glusterfs/kadalu/<vol> → ALL SUCCEED (0.33s each)
06:59:52  ▸ Provisioner enters infinite loop: pgrep→fail→mount→fail→pgrep→fail...
            (cycling through all 33 volumes continuously)
06:59:52  ▸ Two pgrep processes pinned at 90%+ CPU (from accumulating vmexec commands)
~09:42:42 Provisioner log captured (4.7-min sample shows 1,580 mount failures)
09:46:57  Last vmexec command in journal (mount storm still ongoing at showtech capture)
          Total storm duration: 2h 47min, 100K+ commands, 0 successes
```

---

## Cascade Effect

```
Path mismatch in provisioner
    ↓
Every pgrep check fails (wrong path pattern)
    ↓
Every mount attempt fails (directory doesn't exist on host)
    ↓
100K+ failed commands flood vmexec socket
    ↓
2 pgrep processes pinned at 90% CPU each
    ↓
vmexec peaks at 3.1 GB memory, 882 tasks
    ↓
System load reaches 12.77
    ↓
Old buggy code returns mountpoint on failure anyway
    ↓
CreateVolume takes 120s instead of <1s (cycling through 33 failed mounts)
    ↓
Only 20 PVCs created in entire window (metadata written to local unmounted path)
    ↓
126 PVCs stuck in Pending state
    ↓
Dependent pods cannot start
    ↓
Cluster-wide application outage
```

---

## Code-Level Root Cause

### File: `csi/volumeutils.py`
```python
HOSTVOL_MOUNTDIR = "/mnt"          # ← Provisioner's view (container path)
# Node plugin uses "/mnt/cw_glusterfs/kadalu" (host path, via different config)
```

When `CREATE_MOUNT_ON_VMEXEC=True`:
- `execute` is monkey-patched to `execute_vmexec` → commands run on HOST
- `is_gluster_mount_proc_running` is monkey-patched to `is_gl_mount_vmexec` → pgrep on HOST
- BUT `makedirs`, `os.path.exists`, `os.statvfs` still run INSIDE the container
- Container path `/mnt/X` maps to host path `/mnt/cw_glusterfs/kadalu/X` via hostPath volume
- Commands sent to vmexec use the container path, not the host path

### File: `csi/vmexec_socketclient.py`
```python
def is_gl_mount_vmexec(volname, mountpoint):
    # mountpoint = "/mnt/storage-vol1" (container path)
    # But host processes are at "/mnt/cw_glusterfs/kadalu/storage-vol1"
    args = "bin/glusterfs.*{}.*{}".format(safe_volname, safe_mountpoint)
    # Pattern: bin/glusterfs.*glustervol1.*/mnt/storage\-vol1
    # This NEVER matches host processes
    out, err, res = execute_vmexec("/usr/bin/pgrep", "-c", "-f", args)
    return int(out) > 0 and res == 0  # Always returns False
```

### The Old Return-on-Failure Bug (line 993)
```python
def mount_glusterfs(volume, mountpoint, is_client=False):
    if volume['type'] == 'External':
        try:
            return handle_external_volume(...)
        except Exception as mount_err:
            logging.warning("External volume mount failed, returning mountpoint for caller to handle")
            return mountpoint  # ← Returns SUCCESS even on FAILURE
```

---

## Fix Recommendations

### 1. PATH TRANSLATION (Critical — Root Cause Fix)

When `CREATE_MOUNT_ON_VMEXEC=True`, translate container paths to host paths before
sending to vmexec:

```python
# Add at module level
VMEXEC_HOST_MOUNTDIR = os.environ.get("VMEXEC_HOST_MOUNTDIR", "/mnt/cw_glusterfs/kadalu")

def container_to_host_path(container_path):
    """Translate container-namespace path to host-namespace path for vmexec."""
    if vmexecMnt == "True" and container_path.startswith(HOSTVOL_MOUNTDIR + "/"):
        relative = container_path[len(HOSTVOL_MOUNTDIR):]
        return VMEXEC_HOST_MOUNTDIR + relative
    return container_path
```

Then update `handle_external_volume()` and `mount_glusterfs()` to use
`container_to_host_path(mountpoint)` when calling `is_gluster_mount_proc_running()`
and `mount_glusterfs_with_host()`.

### 2. STOP RETURNING MOUNTPOINT ON FAILURE (Already Coded, Not Deployed)

The retry + health-gating fix we already built in `volumeutils.py`:
- `mount_glusterfs()` returns `None` on failure
- All 6 callers gate on `if not mount_glusterfs(): continue`
- Retry with exponential backoff (5 attempts)
- Background reconciler at 60s interval

### 3. PROTECT vmexec FROM COMMAND FLOODS

Add rate-limiting or circuit-breaker for mount attempts per volume to prevent
100K+ command storms.

---

## Files Referenced in This Analysis

| Source | Path in Showtech |
|---|---|
| Provisioner log | `172.22.235.73/.../system/var/log/containers/kadalu-csi-provisioner-0_kadalu_kadalu-provisioner-*.log` |
| Node plugin log | `172.22.235.73/.../system/var/log/containers/kadalu-csi-nodeplugin-*_kadalu_kadalu-nodeplugin-*.log` |
| vmexec journal | `172.22.235.73/.../system/journalctl_vmexec.log` |
| vmexec status | `172.22.235.73/.../system/systemctl_vmexec_status.log` |
| glusterd status | `172.22.235.73/.../system/systemctl_glusterd_status.log` |
| GlusterFS vol info | `172.22.235.73/.../system/gluster_volume_info.log` |
| GlusterFS vol status | `172.22.235.73/.../system/gluster_volume_status.log` |
| System top | `172.22.235.73/.../system/top.log` |
| PVC status | `20260309093233/kubectl-pvc.log` |
| dmesg | `172.22.235.73/.../system/dmesg.log` (no OOM/kernel errors found) |
