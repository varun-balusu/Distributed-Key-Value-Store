#!/bin/bash

set -e

trap 'killall distributedkv' SIGINT

cd $(dirname $0)

killall distributedkv || true
sleep 0.1

go install distribkv/usr/distributedkv

PATH=$PATH:$(dirname $(go list -f '{{.Target}}' .))

gtimeout 10 distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-0.db -http-address=127.0.0.1:8080 -shard=server-0 &

distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-0-r.db -http-address=127.0.0.2:8080 -shard=server-0 -replica &

distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-0-r-2.db -http-address=127.0.0.3:8080 -shard=server-0 -replica &

distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-0-r-3.db -http-address=127.0.0.4:8080 -shard=server-0 -replica &
distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-0-r-4.db -http-address=127.0.0.5:8080 -shard=server-0 -replica &

# distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-1.db -http-address=127.0.0.1:8081 -shard=server-1 &

# distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-1-r.db -http-address=127.0.0.2:8081 -shard=server-1 -replica &

# distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-1-r-2.db -http-address=127.0.0.3:8081 -shard=server-1 -replica &

# distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-2.db -http-address=127.0.0.1:8082 -shard=server-2 &


# distributedkv -db-location=/Users/vbalusu/desktop/distribkv/server-3.db -http-address=127.0.0.1:8083 -shard=server-3 &


wait