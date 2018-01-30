The script in this folder will create a catalog.xml output that can be
used by the thredds server. This script is run every day and will update
the thredds server to show the latest data available.

The script writes the catalog.xml to a folder that is similair to the
example 4.6.8 thredds server.

This script is used with cronjob and the following little script:

```
#!/bin/bash

cd /home/ubuntu
./thredds.sh > thredds/catalog.xml
/usr/sbin/service tomcat8 restart
```

The output of the script can be found at:
https://terraref.ncsa.illinois.edu/thredds/
