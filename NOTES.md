# Implementation Notes

Companion to `README.md` (the Udacity project directions). This file records the design
decisions in the completed code and how to run the whole thing on a modern machine.

## Topic naming convention

Every topic this project owns is namespaced under `org.chicago.cta.`, lower-snake-cased, and
versioned where the payload is a contract with a downstream consumer.

| Topic | Produced by | Format | Partitions |
| --- | --- | --- | --- |
| `org.chicago.cta.station.arrivals.<station_name>` | `producers/models/station.py` | Avro | 1 |
| `org.chicago.cta.turnstile.v1` | `producers/models/turnstile.py` | Avro | 1 |
| `org.chicago.cta.weather.v1` | `producers/models/weather.py` (REST Proxy) | Avro | 1 |
| `org.chicago.cta.stations` | Kafka Connect JDBC source | JSON | 1 |
| `org.chicago.cta.stations.table.v1` | `consumers/faust_stream.py` | JSON | 1 |
| `TURNSTILE_SUMMARY` | KSQL | JSON | — |

Three of these names are not free choices — `consumers/server.py` subscribes to
`org.chicago.cta.weather.v1`, `org.chicago.cta.stations.table.v1`, `TURNSTILE_SUMMARY`, and
the regex `^org.chicago.cta.station.arrivals.`, and that file is starter code.

None of the topics rely on broker auto-creation. Every producer creates its own topic with
explicit partition and replica counts via `AdminClient` in `producers/models/producer.py`,
including the two that are not written by the Python client (weather goes through REST Proxy,
stations through Kafka Connect).

## Decisions worth explaining

**Arrivals get one topic per station; turnstiles share one topic.** The directions ask for a
topic per station for both. That works for arrivals, but a per-station turnstile topic would
force one KSQL table per station, since `CREATE TABLE` reads exactly one topic. Every
turnstile event already carries its own `station_id`, so a single topic aggregates correctly
with `GROUP BY station_id` and stays one table.

**Turnstiles emit one event per rider, not one event carrying a count.** The UI column is a
running total, and KSQL derives it with `COUNT()`. Pre-aggregating in the producer would make
the KSQL table count batches instead of people.

**`prev_station_id` and `prev_direction` are nullable unions.** Both are genuinely null on a
train's first arrival, when the simulation has no previous station to report. Declaring them
as plain `int`/`string` makes the Avro serializer throw on the first event of every run.

**Faust emits every station id, including the 19 on no simulated line.** The CTA table holds
111 distinct station ids; only 92 carry a red, blue or green flag. The other 19 (Brown, Purple
and Pink stops such as Kimball, Southport, Sedgwick and Merchandise Mart) are emitted with an
empty `line` rather than skipped, so that every station id present in the input topic is also
represented in the output topic. Dropping them left the output topic at 92 of 111 -- verified
by consuming the topic and counting distinct ids -- which fails the criterion asking that
"every station ID is represented".

They cost nothing downstream: `consumers/models/lines.py` routes on the line colour and
discards anything that is not red, blue or green, so those records never reach the UI, which
still renders exactly the 94 rows belonging to the three simulated lines.

**Kafka Connect uses JSON on both key and value, with `schemas.enable` off.** Faust consumes
`org.chicago.cta.stations` downstream and cannot decode Confluent-framed Avro without extra
work. Note this means no `stations` schema is registered in Schema Registry — the two are
mutually exclusive, and the starter code and walkthrough both call for JSON here.

**TURNSTILE_SUMMARY aggregates a stream, not the TURNSTILE table.** This is the one place
the project's own design is measurably wrong, so it is worth the detail.

A KSQL table is a changelog: two records sharing a key are an *update*, not an insert. KSQL
also deserializes the Kafka record key as a UTF-8 string, and our keys are Avro-encoded binary
(magic byte, schema id, zigzag varint). Run through UTF-8, distinct keys collapse into each
other wholesale, so counting rows in the TURNSTILE table silently discards most events.

Measured end to end, producer stopped and the pipeline fully drained (consumer group lag
confirmed at 0, so this is loss and not lag):

| Aggregating over | Events counted | of 18,214 |
| --- | --- | --- |
| `CREATE TABLE turnstile` | 5,405 | 30% |
| `CREATE STREAM turnstile_events` | 18,214 | 100% |

Raising the key's resolution does not help. That was the first thing tried -- microsecond
timestamps plus a monotonic guard, verified to produce 4,000 distinct key values across 4,000
consecutive events -- and the counts did not move, because the collision happens in the string
deserialization rather than in the timestamps. That change was reverted; `Turnstile.run()` uses
the inherited `time_millis()` exactly as the starter intends.

So `ksql.py` creates both. TURNSTILE is the table the directions ask for and holds the raw
turnstile data; TURNSTILE_EVENTS is a stream over the same topic, and the summary aggregates
that. A stream is also the truer model of a turnstile entry, which is an event rather than a
state.

**`poll.interval.ms` is 10 minutes.** The station list is static reference data; polling it
on the default 5-second cadence would re-query Postgres ~7000 times a day for no new rows.

**Service URLs come from the environment, defaulting to the README's Host URLs.** This is the
only change to the shape of the starter code, and it is what lets the identical source run
both in the Udacity workspace (`localhost:9092`) and in a container on the compose network
(`kafka0:19092`), where `localhost` would resolve to the container itself.

## One change to the starter's docker-compose.yaml

`confluentinc/cp-ksql-server` has been deleted from Docker Hub, so the starter's compose file
no longer comes up — the 5.2.2 tag it names cannot be pulled at all. Every other 5.2.2 image
in the file is still published and is untouched.

The replacement is `confluentinc/ksqldb-server:0.6.0`, the last release before ksqlDB 0.10
made two breaking changes that this project cannot absorb: tables began requiring a
`PRIMARY KEY` whose type matches the Kafka record key (ours is an Avro `{timestamp}` record,
not an integer `station_id`), and `GROUP BY` columns stopped being written into the record
value. The second one would leave `TURNSTILE_SUMMARY` messages without a `STATION_ID` field,
which `consumers/models/line.py` reads — and that file is starter code.

It takes the same `KSQL_*` environment variables, so nothing else in the service changed. The
matching CLI image is `confluentinc/ksqldb-cli:0.6.0`.

## Running it locally

The pinned dependencies (`confluent-kafka==1.1.0`, `faust==1.7.4`, `pandas==0.24.2`) require
Python 3.7, which no current OS ships. `Dockerfile` + `docker-compose.app.yaml` run the
application code in a Python 3.7 container attached to the Kafka stack, so the pins stay
exactly as the project specifies.

Start everything:

```
docker compose -f docker-compose.yaml -f docker-compose.app.yaml up -d --build
```

Then run the four pieces, each in its own terminal, in this order:

```
docker compose -f docker-compose.yaml -f docker-compose.app.yaml exec app python producers/simulation.py
docker compose -f docker-compose.yaml -f docker-compose.app.yaml exec -w /app/consumers app faust -A faust_stream worker -l info
docker compose -f docker-compose.yaml -f docker-compose.app.yaml exec -w /app/consumers app python ksql.py
docker compose -f docker-compose.yaml -f docker-compose.app.yaml exec -w /app/consumers app python server.py
```

The transit status page is then at http://localhost:8888. Give it a couple of minutes to
settle — duplicate trains on first load are expected while the arrival topics replay.

To run it the way the directions describe instead, on a machine that does have Python 3.7,
nothing here gets in the way: the env-var defaults are already the README's Host URLs, so
`cd producers && pip install -r requirements.txt && python simulation.py` works unchanged.
