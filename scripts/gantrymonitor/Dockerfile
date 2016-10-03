FROM ubuntu
MAINTAINER Max Burnette <mburnet2@illinois.edu>

RUN apt-get -y update \
    && apt-get -y install curl python python-dev python-pip \
    && pip install flask-restful \
    && pip install requests \
    && pip install python-logstash \
    && pip install globusonline-transfer-api-client \
    && mkdir /home/gantry

COPY gantry_monitor_service.py /home/gantry/gantry_monitor_service.py
COPY config_default.json /home/gantry/config_default.json
COPY config_logging.json /home/gantry/config_logging.json

ENV MONITOR_API_PORT 5455
CMD ["python","/home/gantry/gantry_monitor_service.py"]

# BUILD
# docker build -f Dockerfile -t maxzilla2/terra-gantry-monitor .
# docker push maxzilla2/terra-gantry-monitor

# DEPLOY TO COREOS
# ssh -i ~/.ssh/max.key -l core 141.142.208.132
# docker pull maxzilla2/terra-gantry-monitor

# RUN w/ MOUNTED CUSTOM CONFIG, DATA, LOG FOLDERS
# docker run --rm -p 5455:5455
#       -v ~/Git/TERRAref/computing-pipeline/scripts/gantrymonitor/config_custom.json:/home/gantrymonitor/config_custom.json
#       -v ~/Git/TERRAref/computing-pipeline/scripts/gantrymonitor/log:/home/gantrymonitor/log
#       -v ~/gantry:/home/gantry
#       maxzilla2/terra-globus-monitor
# 141.142.208.132:5455
