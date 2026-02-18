ARG builder_version="1.1.0"
FROM dockerhub.cisco.com/robot-dockerprod/kadalu-builder:${builder_version} as builder

ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"

COPY requirements/csi-requirements.txt /tmp/

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install $(grep -vE '^(grpcio|\s*#)' /tmp/csi-requirements.txt) --no-cache-dir && \
    python3 -m pip download --only-binary :all: --dest /tmp/ --no-cache $(grep -oE '^grpcio==.*$' /tmp/csi-requirements.txt) || \
    python3 -m pip install grpcio==1.63.2 --no-cache-dir && \
    python3 -m pip install /tmp/*.whl --force-reinstall && \
    python3 -m pip install netaddr && \
    grep -Po '^[\w\.-]*(?=)' /tmp/csi-requirements.txt | xargs -I pkg python3 -m pip show pkg | grep -P '^(Name|Version|Location)'

FROM dockerhub.cisco.com/robot-dockerprod/cw-ubuntu-base:24.04.20250805-607185b.11 as prod

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -yq && \
    apt-get install -y --no-install-recommends sqlite3 psmisc logrotate xfsprogs attr libtirpc3t64 bash inotify-tools openssh-client liburcu8t64 libgoogle-perftools4t64 && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"

# Copy Python 3.10 runtime from builder (cw-ubuntu-base has no Python)
COPY --from=builder /usr/local/bin/python3.10 /usr/local/bin/python3.10
COPY --from=builder /usr/local/lib/python3.10 /usr/local/lib/python3.10
COPY --from=builder /usr/local/lib/libpython3.10.so.1.0 /usr/local/lib/libpython3.10.so.1.0

# Copy OpenSSL 1.1 libs from builder (Noble ships OpenSSL 3.x, Python 3.10 needs 1.1)
COPY --from=builder /usr/lib/x86_64-linux-gnu/libssl.so.1.1 /usr/lib/x86_64-linux-gnu/libssl.so.1.1
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1 /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1

# Register shared libraries and create python3 symlink
RUN ln -sfn /usr/local/lib/libpython3.10.so.1.0 /usr/local/lib/libpython3.10.so && \
    ln -sfn /usr/local/bin/python3.10 /usr/local/bin/python3 && \
    ldconfig

# copy glusterfs installed in /opt
COPY --from=builder /opt /opt

# actual application to be copied here
# using already installed packages from builder for faster build time
COPY --from=builder /kadalu /kadalu

# Point venv python3 to the copied python3 binary
RUN ln -sfn /usr/local/bin/python3 /kadalu/bin/python3 && \
  mkdir -p /kadalu/volfiles /kadalu/templates && \
  mkdir -p /var/log/glusterfs /var/run/gluster

COPY lib/kadalulib.py          /kadalu/
COPY csi/controllerserver.py   /kadalu/
COPY csi/csi_pb2_grpc.py       /kadalu/
COPY csi/csi_pb2.py            /kadalu/
COPY csi/identityserver.py     /kadalu/
COPY csi/main.py               /kadalu/
COPY csi/start.py              /kadalu/
COPY csi/exporter.py           /kadalu/
COPY csi/nodeserver.py         /kadalu/
COPY csi/volumeutils.py        /kadalu/
COPY csi/vmexec_socketclient.py       /kadalu/
COPY lib/startup.sh            /kadalu/
COPY csi/quota-crawler.sh      /kadalu/
COPY csi/watch-vol-changes.sh  /kadalu/
COPY csi/client_heal.sh        /kadalu/
COPY csi/remove_archived_pv.py /kadalu/
COPY csi/watch-logrotate.sh    /kadalu/

COPY extras/kadalu-logrotate.conf     /kadalu/logrotate.conf

RUN chmod +x /kadalu/startup.sh && \
    chmod +x /kadalu/client_heal.sh

ARG version="(unknown)"
# Container build time (date -u '+%Y-%m-%dT%H:%M:%S.%NZ')
ARG builddate="(unknown)"

LABEL build-date="${builddate}"
LABEL io.k8s.description="KaDalu CSI driver"
LABEL name="kadalu-csi"
LABEL Summary="KaDalu CSI driver"
LABEL vcs-type="git"
LABEL vcs-url="https://github.com/kadalu/kadalu"
LABEL vendor="kadalu"
LABEL version="${version}"

ENTRYPOINT ["python3", "/kadalu/start.py"]

# Debugging, Comment the above line and
# uncomment below line
# ENTRYPOINT ["tail", "-f", "/dev/null"]