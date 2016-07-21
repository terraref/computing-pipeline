# FROM clowder/python-base
FROM ndslabs/plantcv
MAINTAINER Rob Kooper <kooper@illinois.edu>

# Install any programs needed
USER root
RUN apt-get update && apt-get install -y -q build-essential \
       python-opencv \
       git \
    && rm -rf /var/lib/apt/lists/* \
    && git clone https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder.git \
    && cd pyclowder \
    && git checkout bugfix/CATS-554-add-pyclowder-support-for-dataset \
    && python setup.py install \
    && chown -R clowder /home

# Switch to clowder, copy files and be ready to run
USER clowder

# command to run when starting docker
COPY entrypoint.sh *.py /home/clowder/
ENTRYPOINT ["/home/clowder/entrypoint.sh"]
CMD ["extractor"]

# Setup environment variables. These are passed into the container. You can change
# these to your setup. If RABBITMQ_URI is not set, it will try and use the rabbitmq
# server that is linked into the container. MAIN_SCRIPT is set to the script to be
# executed by entrypoint.sh
ENV RABBITMQ_URI="" \
    RABBITMQ_EXCHANGE="clowder" \
    RABBITMQ_VHOST="%2F" \
    RABBITMQ_QUEUE="terra.plantcv" \
    MAIN_SCRIPT="terra.plantcv.py"