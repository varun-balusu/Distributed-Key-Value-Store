#!/bin/bash

set -e

trap 'killall distributedkv' SIGINT

cd $(dirname $0)

killall distributedkv || true
sleep 0.1

go install distribkv/usr/distributedkv

distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-0.db -http-address=127.0.0.1:8080 -shard=Server-0 &

P1=$!

distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-1.db -http-address=127.0.0.1:8081 -shard=Server-1 &

P2=$!

distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-2.db -http-address=127.0.0.1:8082 -shard=Server-2 &

P3=$!

wait $P1 $P2 $P3