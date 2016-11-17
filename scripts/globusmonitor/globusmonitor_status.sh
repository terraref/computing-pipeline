#!/bin/bash
# Status
#   The check result in Nagios convention:
#   0 for OK, 1 for WARNING, 2 for CRITICAL and 3 for UNKNOWN
#
# Item name
#   A name for the check item that is unique within the host. It will be
#   used as Nagios' service description. It must not contain spaces,
#   because spaces separate the four columns.
#
# Performance data
#   You may output zero, one or more performance variables here with the
#   standard Nagios convention. That is varname=value;warn;crit;min;max.
#   If you have no performance data simply write a dash (-) instead. From
#   version 1.1.5 on, it is possible to output more then one variable.
#   Separate your variables with a pipe symbol (|). Do not used spaces.
#
# Check output
#   The last column is the output of the check. It will be displayed in
#   Nagios. Spaces are allowed here and only here.
threshold=4000
status=$( curl -f -s -X GET http://141.142.170.137:5454/status | tr '\n' ' ' )
exitcode=$?
if [ $exitcode -eq 0 ]; then
  unprocessed=$( echo $status | sed 's/.*"unprocessed_task_count": \([0-9]*\).*/\1/' )
  active=$( echo $status | sed 's/.*"active_task_count": \([0-9]*\).*/\1/' )
  echo "0 globusmonitor unprocessed=$unprocessed;5000;10000|active=$active;5000;10000 All is OK."
else
  echo "2 globusmonitor - API is not responding (${exitcode}) : $status"
fi
