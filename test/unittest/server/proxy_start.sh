#!/bin/sh

/bin/rm -rf proxy.log
proxy -p 8082 --config ../server/proxy_config.toml -v=8 -logtostderr=true -n=4 > ../server/proxy.log 2>&1 
sleep 6;
#proxy -p 8082 --config ../server/proxy_config.toml -v=8 -logtostderr=true -n=4
