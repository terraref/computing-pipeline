FROM ubuntu
MAINTAINER Max Burnette <mburnet2@illinois.edu>

RUN apt-get -y update \
    && apt-get -y install curl python python-dev python-pip \
    && pip install flask-restful \
    && pip install requests \
    && pip install python-logstash \
    && pip install urllib3 \
    && pip install globusonline-transfer-api-client

COPY globus_monitor_service.py /home/globusmonitor/globus_monitor_service.py
COPY data /home/globusmonitor/data
COPY config_default.json /home/globusmonitor/config_default.json
COPY config_logging.json /home/globusmonitor/config_logging.json

ENV MONITOR_API_PORT 5454
CMD ["python", "/home/globusmonitor/globus_monitor_service.py"]

# BUILD
# docker build -f Dockerfile -t maxzilla2/terra-globus-monitor .
# docker push maxzilla2/terra-globus-monitor

# DEPLOY TO COREOS
# ssh -i ~/.ssh/max.key -l core 141.142.208.132
# docker pull maxzilla2/terra-globus-monitor

# RUN w/ MOUNTED CUSTOM CONFIG, DATA, LOG FOLDERS
# docker run --rm
#       -v ~/Git/TERRAref/computing-pipeline/scripts/globusmonitor/config_custom.json:/home/globusmonitor/config_custom.json
#       -v ~/Git/TERRAref/computing-pipeline/scripts/globusmonitor/log:/home/globusmonitor/log
#       -v ~/globus:/home/globus
#       maxzilla2/terra-globus-monitor
# 141.142.208.132:5454
