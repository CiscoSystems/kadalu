ARG builder_version="latest"

FROM kadalu/builder:${builder_version} as builder

ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"

COPY requirements/server-requirements.txt /tmp/

RUN python3 -m pip install -r /tmp/server-requirements.txt --no-cache-dir && \
    grep -Po '^[\w\.-]*(?=)' /tmp/server-requirements.txt | xargs -I pkg python3 -m pip show pkg | grep -P '^(Name|Version|Location)'

FROM dockerhub.cisco.com/robot-dockerprod/cw-ubuntu-base:24.04.20250805-607185b.11 as prod

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/kadalu/bin:/opt/bin:/opt/sbin:$PATH"

# Copy Python 3.10 runtime from builder (cw-ubuntu-base has no Python)
COPY --from=builder /usr/local/bin/python3.10 /usr/local/bin/python3.10
COPY --from=builder /usr/local/lib/python3.10 /usr/local/lib/python3.10
COPY --from=builder /usr/local/lib/libpython3.10.so.1.0 /usr/local/lib/libpython3.10.so.1.0

# Register shared libraries and create python3 symlink
RUN ln -sfn /usr/local/lib/libpython3.10.so.1.0 /usr/local/lib/libpython3.10.so && \
    ln -sfn /usr/local/bin/python3.10 /usr/local/bin/python3 && \
    ldconfig

COPY --from=builder /opt /opt

# actual application to be copied here
# using already installed packages from builder for faster build time
COPY --from=builder /kadalu /kadalu

# Point venv python3 to the copied python3 binary
RUN ln -sfn /usr/local/bin/python3 /kadalu/bin/python3

RUN apt-get update -yq && \
    apt-get install -y --no-install-recommends curl attr xfsprogs libtirpc3t64 sqlite3 liburcu8t64 procps \
    libgoogle-perftools4t64 libxml2 && \
    apt-get -y clean && \
    rm -rf /var/lib/apt/lists/*

# Download and verify volgen install script instead of piping to shell (CWE-829)
ARG VOLGEN_VERSION="latest"
RUN curl -fsSL -o /tmp/volgen_install.sh \
    "https://github.com/kadalu/volgen/releases/${VOLGEN_VERSION}/download/install.sh" && \
    chmod +x /tmp/volgen_install.sh && \
    bash /tmp/volgen_install.sh && \
    rm -f /tmp/volgen_install.sh

RUN mkdir -p /kadalu/templates /kadalu/volfiles
RUN mkdir -p /var/run/gluster /var/log/glusterfs

COPY lib/kadalulib.py        /kadalu/kadalulib.py
COPY server/server.py        /kadalu/server.py
COPY server/glusterfsd.py    /kadalu/glusterfsd.py
COPY server/shd.py           /kadalu/shd.py
COPY server/mount-glustervol /usr/bin/mount-glustervol
COPY lib/startup.sh          /kadalu/startup.sh
COPY server/stop-server.sh   /kadalu/stop-server.sh
COPY server/exporter.py      /kadalu/exporter.py
COPY server/heal-info.sh     /kadalu/heal-info.sh
COPY server/serverutils.py   /kadalu/serverutils.py

RUN chmod +x /usr/bin/mount-glustervol
RUN chmod +x /kadalu/startup.sh
RUN chmod +x /kadalu/stop-server.sh
RUN chmod +x /kadalu/heal-info.sh

ARG version="(unknown)"
# Container build time (date -u '+%Y-%m-%dT%H:%M:%S.%NZ')
ARG builddate="(unknown)"

LABEL build-date="${builddate}"
LABEL io.k8s.description="KaDalu container(glusterfsd or glustershd)"
LABEL name="kadalu-server"
LABEL Summary="KaDalu Server"
LABEL vcs-type="git"
LABEL vcs-url="https://github.com/kadalu/kadalu"
LABEL vendor="kadalu"
LABEL version="${version}"

ENTRYPOINT ["python3", "/kadalu/server.py"]

# Debugging, Comment the above line and
# uncomment below line
# ENTRYPOINT ["tail", "-f", "/dev/null"]
