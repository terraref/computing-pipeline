FROM ubuntu
MAINTAINER Max Burnette <mburnet2@illinois.edu>

RUN apt-get -y update \
    && apt-get -y install curl python python-dev python-pip \
    && pip install flask-restful \
    && pip install requests \
    && pip install urllib3 \
    && pip install globusonline-transfer-api-client

#COPY globus_monitor_service.py /home/globusmonitor/globus_monitor_service.py
#COPY log /home/globusmonitor/log
#COPY config/default.json /home/globusmonitor/config/default.json

COPY / /home/globusmonitor

ENV MONITOR_API_PORT 5454
CMD ["python", "/home/globusmonitor/globus_monitor_service.py"]


# docker build -f Dockerfile -t maxzilla2/terra-globus-monitor .
# docker run --rm -v config/custom.json:/home/globusmonitor/config/custom.json -v ~/globus:/home/globus maxzilla2/terra-globus-monitor

# Push to dockerhub
# On CoreOS, pull from dockerhub & run
#   ssh -i ~/.ssh/max.key -l core 141.142.208.132
#   docker pull ...
# Try to access at - 141.142.208.132:5455
# Needs to mount a volume where the globus stuff will live
