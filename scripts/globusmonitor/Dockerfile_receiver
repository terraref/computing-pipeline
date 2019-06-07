FROM terraref/terrautils:1.2
MAINTAINER Max Burnette <mburnet2@illinois.edu>

RUN apt-get -y update \
    && apt-get -y install curl \
    && pip install flask-restful \
        python-logstash \
        globusonline-transfer-api-client \
        psycopg2

COPY *.py *.json *.pem /home/globusmonitor/

ENV MONITOR_API_PORT 5454
CMD ["python", "/home/globusmonitor/globus_monitor_service.py"]
