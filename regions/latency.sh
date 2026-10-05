#!/bin/bash
# Sets one-way latency on every cross-region link (toxiproxy), so a round
# trip costs 2 x MS. 40 is roughly us-east <-> eu-west (~80ms RTT).
# Usage: regions/latency.sh <ms>      regions/latency.sh 0   removes it
#        regions/latency.sh cut       partitions the regions (links hang)
#        regions/latency.sh heal      undoes cut
set -eu
api=http://localhost:8474
for p in us_shard0 us_shard1 eu_shard2 us_replication us_api eu_api; do
  for t in latency_up latency_down cut; do curl -s -o /dev/null -X DELETE $api/proxies/$p/toxics/$t; done
  case "$1" in
    heal) ;;
    cut) # timeout=0: data stops flowing, connections hang. A real partition, not a fast refusal.
      curl -s -o /dev/null -X POST $api/proxies/$p/toxics -d '{"name":"cut","type":"timeout","attributes":{"timeout":0}}' ;;
    0) ;;
    *) for dir in up down; do
         curl -s -o /dev/null -X POST $api/proxies/$p/toxics \
           -d "{\"name\":\"latency_$dir\",\"type\":\"latency\",\"stream\":\"${dir}stream\",\"attributes\":{\"latency\":$1}}"
       done ;;
  esac
done
echo "cross-region links: $1"
