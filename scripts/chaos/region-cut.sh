#!/bin/bash
# Cuts the link between regions for 30s while eu users buy eu events (local)
# and, at a steady 1000 arrivals/s, us events (forwarded across the link).
# Pass: local traffic is untouched; forwarded requests get fast 503s during
# the cut and recover after it, instead of hanging.
# Usage: scripts/chaos/region-cut.sh   (stack up with app-eu replicas)
set -u
cd "$(dirname "$0")/../.."
ids() { node --input-type=module -e "import { partitionOf } from './src/shards.ts';
  console.log(Array.from({ length: 200 }, (_, i) => i + 1).filter((e) => (partitionOf(e, 0) % 3 === 2) === ($1)).join(','))" 2>/dev/null; }
EU=$(ids true); US=$(ids false)
node scripts/seed.ts 200 >/dev/null; regions/latency.sh 40 >/dev/null; sleep 2
docker run --rm --network seatrush_default -v $PWD/load:/load -e BASE_URL=http://nginx-eu -e VUS=100 -e DURATION=70s -e EVENT_IDS=$EU \
  grafana/k6 run --quiet --summary-export /load/out/eu-local.json /load/book.js >/dev/null 2>&1 &
docker run --rm --network seatrush_default -v $PWD/load:/load -e BASE_URL=http://nginx-eu -e RATE=1000 -e DURATION=68s -e EVENT_IDS=$US \
  grafana/k6 run --quiet --summary-export /load/out/eu-to-us.json /load/spike.js >/dev/null 2>&1 &
sleep 20; regions/latency.sh cut; sleep 30; regions/latency.sh heal; regions/latency.sh 40 >/dev/null
wait
for f in eu-local eu-to-us; do node -e "const m=require('./load/out/$f.json').metrics,d=m.http_req_duration,c=k=>m[k]?.count??0;
  console.log('$f'.padEnd(9),'requests',m.http_reqs.count,'failed',(m.http_req_failed.value*100).toFixed(1)+'%','p50',Math.round(d['p(50)'])+'ms',
  'p99',Math.round(d['p(99)'])+'ms','max',Math.round(d.max)+'ms',c('rejected')?'fast 503 '+c('rejected'):'',c('dropped_iterations')?'never sent '+c('dropped_iterations'):'')"; done
