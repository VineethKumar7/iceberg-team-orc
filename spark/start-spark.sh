#!/bin/bash

# Start the Spark master and worker. Adjust according to your setup.
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://master:7077

# Keep the container running so that Spark does not exit
tail -f /dev/null