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
# docker run --rm -p 5455:5455
#       -v ~/Git/TERRAref/computing-pipeline/scripts/gantrymonitor/config_custom.json:/home/gantrymonitor/config_custom.json
#       -v ~/Git/TERRAref/computing-pipeline/scripts/gantrymonitor/log:/home/gantrymonitor/log
#       -v ~/gantry:/home/gantry
#       maxzilla2/terra-globus-monitor

docker run -p 5455:5455 --add-host="localhost:141.142.22.37" \
	-v /var/log/gantrymonitor:/home/gantry/data \
	-v /gantry_data:/home/gantry/sites \
	-v /gantry_data/LemnaTec/ToDelete/sites:/home/gantry/delete/sites \
	-v /var/log:/var/log \
-d maxzilla2/terra-gantry-monitor

