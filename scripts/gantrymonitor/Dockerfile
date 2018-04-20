FROM ubuntu
MAINTAINER Max Burnette <mburnet2@illinois.edu>

RUN apt-get -y update \
    && apt-get -y install curl \
        python \
        python-dev \
        python-pip \
    && pip install flask-restful \
        requests \
        python-logstash \
        globusonline-transfer-api-client \
    && mkdir /home/gantry

COPY start_services.sh *.py *.json /home/gantry/

CMD ["/home/gantry/start_services.sh"]

ENV MONITOR_API_PORT=5455

# RUN w/ MOUNTED CUSTOM CONFIG, DATA, LOG FOLDERS
# docker run -p 5455:5455 \
#	-v /var/log/gantrymonitor:/home/gantry/data \
#	-v /gantry_data:/home/gantry/sites \
#	-v /gantry_data/LemnaTec/ToDelete/sites:/home/gantry/delete/sites \
#	-v /var/log:/var/log \
# -d terraref/terra-gantry-monitor

