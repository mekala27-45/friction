# Files in a landing volume, not a message bus

Status: accepted, 29 October 2026.

Sources arrive as gzip CSV files in a Unity Catalog volume, one folder per drop, read by Auto Loader into streaming tables.

## Alternatives

- **Kafka or Kinesis into streaming tables.** What a card authorization stream looks like in production. Lost because no broker exists on a free tier the build can own, and a broker run for one day on a laptop would be a demo of a broker, not of the lakehouse.
- **Direct table writes by the replay.** Simplest. Lost because it skips the part that breaks in real life: files arriving late, twice, renamed or with a column changed.

## Why files

Most bank lakehouses ingest batch extracts and CDC exports as files, and the failures worth testing (a resent file, a renamed column, a shifted clock) are file failures. Auto Loader with rescued data and file metadata gives the bronze layer what a stream consumer would also need: what arrived, when, and from where.

## What it does not show

Exactly once delivery from a broker, consumer lag, and ordering within a partition. The README says so.
