#!/usr/bin/env bash

python /home/gantry/gantry_scanner_service.py &
python /home/gantry/globus_manager_service.py &
wait
