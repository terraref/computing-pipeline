FROM geodata/gdal
MAINTAINER Max Burnette <mburnet2@illinois.edu>

# ------------------------
# This Dockerfile requires scripts from both the main stereoImager directory and the /extractor sub-directory.
# From main stereoImager directory:
#     docker build -f extractor/Dockerfile -t terra-demosaic .
#     docker run -p 9000:9000 -e RABBITMQ_URI=amqp://user:pass@<ip>/%2f terra-demosaic
# ------------------------

# Install any programs needed
USER root
RUN apt-get update && apt-get install -y -q build-essential \
       git \
       python-setuptools python-dev build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && easy_install pip \
    && pip install pika requests numpy scipy pillow \
    && git clone https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder.git \
    && cd pyclowder \
    && git checkout bugfix/CATS-554-add-pyclowder-support-for-dataset \
    && python setup.py install

# command to run when starting docker
COPY /extractor/entrypoint.sh /extractor/extractor_info.json /extractor/*.py /home/extractor/
COPY bin_to_geotiff.py /home/
ENTRYPOINT ["/home/extractor/entrypoint.sh"]
CMD ["extractor"]

# Setup environment variables. These are passed into the container. You can change
# these to your setup. If RABBITMQ_URI is not set, it will try and use the rabbitmq
# server that is linked into the container. MAIN_SCRIPT is set to the script to be
# executed by entrypoint.sh
ENV RABBITMQ_EXCHANGE="clowder" \
    RABBITMQ_VHOST="%2F" \
    RABBITMQ_QUEUE="terra.demosaic" \
    MAIN_SCRIPT="terra.demosaic.py"