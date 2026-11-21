# young-coin-collector data

Append-only research data for the collector on `main`.
Do not merge this branch into `main` (unrelated history).

Read it with `DATA_DIR=<this directory> python3 status.py` from a checkout of `main`.
See the main branch README for the cohort definition, the filter, and how to stop the workflow.

Layout: `shards/YYYY-MM-DD/` (full record), `mutable/` (resume snapshots), `heartbeat.json`.
A `STOP` file here makes the next Actions run exit without collecting or chaining.
