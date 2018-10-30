FROM terraref/terrautils:1.2
MAINTAINER Max Burnette <mburnet2@illinois.edu>

RUN apt-get -y update \
    && apt-get -y install curl \
    && pip install flask-restful \
        python-logstash \
        psycopg2 \
        pandas

COPY *.py *.json /home/filecounter/

ENV MONITOR_API_PORT 5454
CMD ["python", "/home/filecounter/filecounter.py"]
