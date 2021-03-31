#!/bin/bash

set -e

trap 'killall distributedkv' SIGINT

cd $(dirname $0)

killall distributedkv || true
sleep 0.1

go install distribkv/usr/distributedkv

PATH=$PATH:$(dirname $(go list -f '{{.Target}}' .))

# proccesses to be purged 
distributedkv -purge -db-location=/Users/vbalusu/desktop/distribkv/server-0.db -http-address=127.0.0.1:8080 -shard=server-0 
distributedkv -purge -db-location=/Users/vbalusu/desktop/distribkv/server-1.db -http-address=127.0.0.1:8081 -shard=server-1
distributedkv -purge -db-location=/Users/vbalusu/desktop/distribkv/server-2.db -http-address=127.0.0.1:8082 -shard=server-2 
distributedkv -purge -db-location=/Users/vbalusu/desktop/distribkv/server-3.db -http-address=127.0.0.1:8083 -shard=server-3 
