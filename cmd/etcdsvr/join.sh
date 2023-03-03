#!/bin/sh

base=$(dirname "$0")

cd ${base}
pwd

./shutdown.sh
sleep 5

./tool.py join

