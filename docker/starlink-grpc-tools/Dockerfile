FROM python:3.7-bullseye

RUN apt-get update
RUN apt-get install -y git
RUN printf '#!/bin/sh\nexit 0' > /usr/sbin/policy-rc.d

WORKDIR /starlink

RUN git clone https://github.com/sparky8512/starlink-grpc-tools.git
WORKDIR /starlink/starlink-grpc-tools/
RUN pip install -r requirements.txt
RUN pip install redis 
COPY docker/starlink-grpc-tools/check_grpc.py check_grpc.py  

ENTRYPOINT [ "python", "check_grpc.py", "dish_grpc_mqtt.py", "--port", "1883", "-t", "2", "status", "-v"]
# ENTRYPOINT ["/bin/bash"]