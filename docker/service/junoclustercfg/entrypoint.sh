#! /bin/bash

cd /opt/juno

if [ ! -p ./log ]; then
    mkfifo ./log
fi

cp /opt/juno/config.toml /opt/juno/enabled_config.toml

if [ ! -z "$NUM_ZONES" ]; then
    sed -i s/NumZones.*$/NumZones=${NUM_ZONES}/g /opt/juno/enabled_config.toml
fi

if [ ! -z "$SS_HOSTS" ]; then
    sed -i s/SSHosts.*$/SSHosts=${SS_HOSTS}/g /opt/juno/enabled_config.toml
fi

"$@"

while true; do
    tail -f ./log
done

