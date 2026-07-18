#!/usr/bin/env bash
#
# Narrated Elasticsearch 7.16 demo: setup -> ingest -> search.
# Run from the es-tutorial/ directory:  ./demo.sh
#
# Each step pauses so you can talk through it. Press ENTER to continue.
set -euo pipefail

ES=${ES:-localhost:9200}
IDX=${IDX:-drivers}
CENTER_LAT=23.8103   # Dhaka center
CENTER_LON=90.4125

pause() { echo; read -rp "  ↵ press ENTER for: $1"; echo; }
run()   { echo "  \$ $*"; eval "$*"; echo; }

echo "=================================================================="
echo " Elasticsearch 7.16 demo — cluster -> ingest -> search"
echo " ES=$ES  index=$IDX"
echo "=================================================================="

# ---------------------------------------------------------------- 1. cluster
pause "STEP 1 — bring up the cluster & check health"
run "docker compose up -d"
echo "  waiting for Elasticsearch to answer..."
until curl -s "$ES/_cluster/health" >/dev/null 2>&1; do sleep 2; done
run "curl -s '$ES?pretty' | grep -E '\"number\"|\"cluster_name\"'"
run "curl -s '$ES/_cluster/health?pretty' | grep -E 'cluster_name|status|number_of_nodes'"
run "curl -s '$ES/_cat/nodes?v'"

# ---------------------------------------------------------------- 2. mapping
pause "STEP 2 — create the index with a geo_point mapping"
run "curl -s -X DELETE '$ES/$IDX' >/dev/null 2>&1 || true"
run "curl -s -X PUT '$ES/$IDX' -H 'Content-Type: application/json' -d '{
  \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 },
  \"mappings\": { \"properties\": {
    \"driver_name\":   { \"type\": \"text\", \"fields\": { \"keyword\": { \"type\": \"keyword\" } } },
    \"driver_rating\": { \"type\": \"float\" },
    \"driver_review\": { \"type\": \"text\" },
    \"phone\":         { \"type\": \"keyword\" },
    \"location\":      { \"type\": \"geo_point\" }
  } }
}'"
echo
run "curl -s '$ES/$IDX/_mapping?pretty'"

# ---------------------------------------------------------------- 3. ingest
pause "STEP 3 — ingest driver documents (bulk API)"
run "curl -s -X POST '$ES/$IDX/_bulk?refresh=true' -H 'Content-Type: application/json' --data-binary @drivers.bulk.json | grep -oE '\"errors\":(true|false)'"
run "curl -s '$ES/$IDX/_count'"

# ---------------------------------------------------------------- 4. search
pause "STEP 4a — match_all (see every document)"
run "curl -s '$ES/$IDX/_search?pretty' -H 'Content-Type: application/json' -d '{\"query\":{\"match_all\":{}}}' | grep '\"driver_name\"'"

pause "STEP 4b — full-text search on the review field"
run "curl -s '$ES/$IDX/_search?pretty' -H 'Content-Type: application/json' -d '{\"query\":{\"match\":{\"driver_review\":\"safely\"}}}' | grep -E '\"driver_name\"|\"_score\"'"

pause "STEP 4c — GEO: drivers within 5km, nearest first"
run "curl -s '$ES/$IDX/_search?pretty' -H 'Content-Type: application/json' -d '{
  \"query\": { \"bool\": { \"filter\": { \"geo_distance\": {
    \"distance\": \"5km\", \"location\": { \"lat\": $CENTER_LAT, \"lon\": $CENTER_LON } } } } },
  \"sort\": [ { \"_geo_distance\": { \"location\": { \"lat\": $CENTER_LAT, \"lon\": $CENTER_LON }, \"order\": \"asc\", \"unit\": \"km\" } } ]
}' | grep -E '\"driver_name\"|\"sort\"'"

pause "STEP 4d — GEO + rating filter: within 15km AND rating >= 4.6"
run "curl -s '$ES/$IDX/_search?pretty' -H 'Content-Type: application/json' -d '{
  \"query\": { \"bool\": { \"filter\": [
    { \"geo_distance\": { \"distance\": \"15km\", \"location\": { \"lat\": $CENTER_LAT, \"lon\": $CENTER_LON } } },
    { \"range\": { \"driver_rating\": { \"gte\": 4.6 } } } ] } },
  \"sort\": [ { \"_geo_distance\": { \"location\": { \"lat\": $CENTER_LAT, \"lon\": $CENTER_LON }, \"order\": \"asc\", \"unit\": \"km\" } } ]
}' | grep -E '\"driver_name\"|\"driver_rating\"'"

echo "=================================================================="
echo " Done. Same geo query from Go:  go run ./cmd/driverapi"
echo "   curl 'localhost:8090/drivers/nearby?lat=$CENTER_LAT&lon=$CENTER_LON&radius_km=5'"
echo "=================================================================="
