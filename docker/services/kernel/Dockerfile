FROM python:3.7-bullseye

RUN apt-get update
RUN apt-get install -y git
RUN printf '#!/bin/sh\nexit 0' > /usr/sbin/policy-rc.d

WORKDIR /leotest

RUN pip install pyyaml azure-storage-blob grpcio protobuf \
        docker tenacity pyroute2 python-dateutil
COPY services/kernel /leotest/services/kernel
COPY common /leotest/common/
COPY certs /leotest/certs/

ENTRYPOINT [ "python", "-m", "services.kernel"]
# ENTRYPOINT ["/bin/bash"]