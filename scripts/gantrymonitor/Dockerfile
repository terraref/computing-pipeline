FROM ubuntu
MAINTAINER Max Burnette <mburnet2@illinois.edu>

RUN apt-get -y update \
    && apt-get -y install curl python python-dev python-pip \
    && pip install flask-restful \
    && pip install requests \
    && pip install globusonline-transfer-api-client

#COPY gantry_monitor_service.py /home/gantrymonitor/gantry_monitor_service.py
#COPY log /home/gantrymonitor/log
#COPY config/default.json /home/gantrymonitor/config/default.json

COPY / /home/gantrymonitor

ENV MONITOR_API_PORT 5455
CMD ["python", "/home/gantrymonitor/gantry_monitor_service.py"]


# docker build -f Dockerfile -t maxzilla2/terra-gantry-monitor .
# docker run --rm -v config/custom.json:/home/gantrymonitor/config/custom.json -v ~/gantry:/home/gantry maxzilla2/terra-gantry-monitor

# Push to dockerhub
# On CoreOS, pull from dockerhub & run
#   ssh -i ~/.ssh/max.key -l core 141.142.208.132
#   docker pull ...
# Try to access at - 141.142.208.132:5455
# Needs to mount a volume where the gantry stuff will live
