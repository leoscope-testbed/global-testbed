FROM python:3.7-bullseye

RUN apt-get update
RUN apt-get install -y git
RUN printf '#!/bin/sh\nexit 0' > /usr/sbin/policy-rc.d

WORKDIR /leotest

# RUN apt install -y iputils-ping netperf flent pkg-config m4 libtool automake autoconf hping3 iperf3
RUN pip install pyyaml azure-storage-blob pyroute2 grpcio pymongo protobuf docker crontab croniter python-jose

COPY orchestrator /leotest/orchestrator/
COPY common /leotest/common/
COPY certs /leotest/certs/

ENTRYPOINT [ "python", "-m", "orchestrator"]
# ENTRYPOINT ["/bin/bash"]