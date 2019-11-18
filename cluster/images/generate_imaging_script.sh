#!/usr/bin/env bash
#
# Generate a creation script for a VM and image

ZONE="europe-west4-a"
DEV_PROJECT="hmf-pipeline-development" 
PROJECT="${DEV_PROJECT}"

if [ -n "$1" ]
then
  PROJECT=$1
fi

GCL="gcloud beta compute --project=${PROJECT}"

which gcloud 2>&1 >/dev/null
[[ $? -ne 0 ]] && echo "gcloud is missing" >&2 && exit 1

type="standard"
cmds="$(dirname "$0")/${type}.cmds"
[[ ! -f ${cmds} ]] && echo "Cannot find commands file '${type}'!" >&2 && exit 1
sourceInstance="diskimager-${type}"

# From here on in we want to fail immediately on any surprises
echo "#!$(which sh) -e"

# Defaults to Debian 9; we assume that for now
# Also this will fail if there are underscores in the the name, and will succeed if there are hyphens, which doesn't really agree
# with the regex given in the help
network="--network=diskimager --subnet=diskimager"
[[ "${PROJECT}" = "${DEV_PROJECT}" ]] && network=""
echo "$GCL instances create ${sourceInstance} --description=\"Instance for ${type} disk image creation\" --zone=${ZONE} ${network}"
echo "sleep 5"
# Ignore lines consisting only of spaces, or those beginning with '#'
tunnel="--tunnel-through-iap"
[[ "${PROJECT}" = "${DEV_PROJECT}" ]] && tunnel=""
egrep -v '^#|^ *$' ${cmds} | while read cmd
do
  echo "$GCL ssh ${sourceInstance} --zone=${ZONE} --command=\"$cmd\" ${tunnel}"
done

echo "$GCL instances stop ${sourceInstance} --zone=${ZONE}"
echo "$GCL images create ${sourceInstance}-$(date +%Y%m%d%H%M) --family=${sourceInstance} --source-disk=${sourceInstance} --source-disk-zone=${ZONE}"
echo "$GCL instances -q delete ${sourceInstance} --zone=${ZONE}"
