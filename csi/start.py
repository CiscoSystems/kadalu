import os

from kadalulib import Monitor, Proc, logging_setup


def main():
    curr_dir = os.path.dirname(__file__)

    mon = Monitor()
    mon.add_process(Proc("csi", "python3", [curr_dir + "/main.py"]))
    mon.add_process(Proc("metrics", "python3", [curr_dir + "/exporter.py"]))
    mon.add_process(Proc("volumewatch", "bash", [curr_dir + "/watch-vol-changes.sh"]))
    mon.add_process(Proc("kadalu-logrotate", "bash", [curr_dir + "/watch-logrotate.sh"]))

    if os.environ.get("CSI_ROLE", "-") == "provisioner":
        mon.add_process(Proc("quota", "bash", [curr_dir + "/quota-crawler.sh"]))

    # The nodeplugin runs on every node (DaemonSet). On IPv6-only clusters it
    # reconciles the host /etc/hosts gluster-node-<N> mapping (published by the
    # orchestrator as the kadalu-gluster-hosts ConfigMap) so the host-side
    # glusterfs mount can resolve the volfile servers. No-op when the mapping
    # ConfigMap / host mounts are absent (e.g. IPv4 or provisioner role).
    if os.environ.get("CSI_ROLE", "-") == "nodeplugin":
        mon.add_process(Proc("gluster-hosts-sync", "bash",
                             [curr_dir + "/sync-gluster-hosts.sh"]))

    mon.start_all()
    mon.monitor()


if __name__ == "__main__":
    logging_setup()
    main()
