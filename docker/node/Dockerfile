FROM python:3.7-bullseye

RUN apt-get update
RUN apt-get install -y git
RUN printf '#!/bin/sh\nexit 0' > /usr/sbin/policy-rc.d

WORKDIR /leotest

RUN pip install pyyaml azure-storage-blob pyroute2 grpcio pymongo protobuf \
        docker python-crontab tenacity croniter pymemcache paho-mqtt redis python-atd
RUN apt install -y cron at

RUN git clone https://github.com/sparky8512/starlink-grpc-tools.git
RUN pip install -r starlink-grpc-tools/requirements.txt
RUN pip install skyfield 
RUN pip install requests

COPY node /leotest/node/
COPY common /leotest/common/
COPY certs /leotest/certs/
COPY ext_depen /leotest/ext_depen/
COPY experiment_configs /leotest/experiment_configs/

ENTRYPOINT [ "python", "-m", "node"]
#ENTRYPOINT ["/bin/bash"]