# Elasticsearch 7.16 Local Demo — Setup → Ingest → Search

A hands-on walkthrough for a single-node Elasticsearch 7.16 cluster: bring it
up locally, ingest driver documents (with geo data), and search them —
including geo queries by `curl` and from a small Go HTTP endpoint.

- **Elasticsearch 7.16.3** → http://localhost:9200
- **Kibana 7.16.3** → http://localhost:5601 (Dev Tools console: http://localhost:5601/app/dev_tools#/console)
- Security disabled and disk watermark turned off — **tutorial only**, not production.

> Live demo: run `./demo.sh` for a narrated, step-by-step walk through everything below.

---

## 1. Set up the local cluster

```bash
cd es-tutorial
docker compose up -d
```

Wait for it to report healthy, then check the cluster:

```bash
curl "localhost:9200/_cluster/health?pretty"      # status: green/yellow
curl "localhost:9200/_cat/nodes?v"                # the node(s) in the cluster
curl "localhost:9200"                             # version info
```

A single-node cluster shows `status: yellow` once you have replicas with
nowhere to go — that's expected and fine for a demo.

Tear down with `docker compose down` (keeps data) or `docker compose down -v`
(wipes data).

---

## 2. Create the index (mapping)

`location` is a **`geo_point`**, so geo queries work directly on it. The
`{ "lat", "lon" }` object is Elasticsearch's native geo_point format.

```bash
curl -X PUT "localhost:9200/drivers" -H 'Content-Type: application/json' -d '{
  "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
  "mappings": {
    "properties": {
      "driver_name":   { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "driver_rating": { "type": "float" },
      "driver_review": { "type": "text" },
      "phone":         { "type": "keyword" },
      "location":      { "type": "geo_point" }
    }
  }
}'
```

Document shape:

```json
{
  "driver_name":   "Karim Rahman",
  "driver_rating": 4.8,
  "driver_review": "Very polite and drives safely",
  "phone":         "+8801710000001",
  "location":      { "lat": 23.8103, "lon": 90.4125 }
}
```

---

## 3. Ingest documents

**One document:**

```bash
curl -X POST "localhost:9200/drivers/_doc/1?refresh=true" -H 'Content-Type: application/json' -d '{
  "driver_name":"Karim Rahman","driver_rating":4.8,
  "driver_review":"Very polite and drives safely",
  "phone":"+8801710000001",
  "location":{"lat":23.8103,"lon":90.4125}
}'
```

**Many at once (bulk API)** — see `drivers.bulk.json` for the full sample set:

```bash
curl -X POST "localhost:9200/drivers/_bulk?refresh=true" \
  -H 'Content-Type: application/json' --data-binary @drivers.bulk.json
```

Verify:

```bash
curl "localhost:9200/drivers/_count"                 # doc count
curl "localhost:9200/drivers/_mapping?pretty"        # confirm geo_point
```

---

## 4. Search

### Match all / by field

```bash
curl "localhost:9200/drivers/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'

curl "localhost:9200/drivers/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "match": { "driver_review": "safely" } }
}'
```

### Geo: `geo_distance` — within a radius, nearest first

```bash
curl "localhost:9200/drivers/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "bool": { "filter": { "geo_distance": {
    "distance": "5km",
    "location": { "lat": 23.8103, "lon": 90.4125 }
  } } } },
  "sort": [ { "_geo_distance": {
    "location": { "lat": 23.8103, "lon": 90.4125 },
    "order": "asc", "unit": "km"
  } } ]
}'
```

### Geo: `geo_bounding_box` — inside a rectangle

```bash
curl "localhost:9200/drivers/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "geo_bounding_box": { "location": {
    "top_left":     { "lat": 23.90, "lon": 90.35 },
    "bottom_right": { "lat": 23.70, "lon": 90.45 }
  } } }
}'
```

### Geo + rating — nearby AND highly rated

```bash
curl "localhost:9200/drivers/_search?pretty" -H 'Content-Type: application/json' -d '{
  "query": { "bool": { "filter": [
    { "geo_distance": { "distance": "10km", "location": { "lat": 23.8103, "lon": 90.4125 } } },
    { "range": { "driver_rating": { "gte": 4.5 } } }
  ] } },
  "sort": [ { "_geo_distance": { "location": { "lat": 23.8103, "lon": 90.4125 }, "order": "asc", "unit": "km" } } ]
}'
```

---

## 5. Query it from Go (HTTP endpoint)

Package `internal/driver` wraps the geo_distance search; `cmd/driverapi`
exposes it over HTTP. From the repo root:

```bash
go run ./cmd/driverapi        # listens on :8090
```

```bash
# drivers within 5km of Dhaka center
curl "localhost:8090/drivers/nearby?lat=23.8103&lon=90.4125&radius_km=5"

# within 15km, rating >= 4.6
curl "localhost:8090/drivers/nearby?lat=23.8103&lon=90.4125&radius_km=15&min_rating=4.6"
```

Response (each driver carries `distance_km` from the ES `_geo_distance` sort):

```json
{
  "count": 3,
  "drivers": [
    { "driver_name": "Karim Rahman", "driver_rating": 4.8, "distance_km": 0, "location": { "lat": 23.8103, "lon": 90.4125 }, "...": "..." }
  ]
}
```

Env overrides: `ES_URL` (default `http://localhost:9200`), `ES_INDEX`
(default `drivers`), `ADDR` (default `:8090`).

---

## Troubleshooting

- **`read-only-allow-delete` / flood-stage block on writes** — the host disk
  crossed the 95% flood watermark and ES locked the index read-only. Clear it:
  ```bash
  curl -X PUT "localhost:9200/_all/_settings" -H 'Content-Type: application/json' \
    -d '{"index.blocks.read_only_allow_delete": null}'
  ```
  This compose already sets `cluster.routing.allocation.disk.threshold_enabled=false`
  to avoid re-triggering, but free up disk for real work.
- **Kibana slow to load** — it takes ~30–60s after the container starts.
