#!/usr/bin bash

# Download and install Globus official python API client
# todo: could just use easy-install or pip if available
git clone https://github.com/globusonline/transfer-api-client-python.git
cd transfer-api-client-python
python setup.py install
