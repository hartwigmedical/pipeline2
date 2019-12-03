#!/bin/bash

/usr/bin/java ${JAVA_OPTS} --illegal-access=permit -jar /usr/share/hartwig/bootstrap.jar "$@"
status=$?
if [ ${status} -ne 0 ]; then
  echo "Failed to start bootstrap: $status"
  exit ${status}
fi