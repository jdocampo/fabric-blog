---
title: 'OPTIMIZE and the ConcurrentAppendException nobody warned you about'
description: 'Compaction and streaming writes on the same Delta table will collide. The fix is partition-scoped OPTIMIZE, not a retry loop.'
pubDate: 2026-03-11
tags: ['delta-lake', 'spark']
---

A nightly maintenance notebook runs `OPTIMIZE` over a Delta table. A streaming job appends
to the same table every few minutes. Roughly once a week the append fails:

```
ConcurrentAppendException: Files were added to the root of the table by a
concurrent update. Please try the operation again.
```

The retry usually succeeds, which is why this sits in a backlog for months instead of
being fixed.

## Why it happens

Delta uses optimistic concurrency control. Both writers read the same table version,
do their work, then try to commit. At commit time the loser checks whether the winner
touched files that overlap its own read set. Compaction rewrites files across the whole
table, so its read set is the whole table — every concurrent append overlaps it.

The exception is not a bug. It is the protocol doing its job.

## The fix

Scope the compaction so its read set and the writer's read set cannot intersect. If the
table is partitioned by date and the stream only ever appends to today, compact
everything except today:

```python
from delta.tables import DeltaTable
from datetime import date, timedelta

cutoff = (date.today() - timedelta(days=1)).isoformat()

DeltaTable.forName(spark, "lakehouse.sales_events") \
    .optimize() \
    .where(f"event_date < '{cutoff}'") \
    .executeCompaction()
```

The `where` clause is the whole point. Without it the operation declares a dependency on
every file in the table.

> If the stream can write to arbitrary partitions, no predicate will save you. Give
> maintenance an exclusive window instead, or move compaction behind the same lock as
> the writer.

## What not to do

| Approach | Why it fails |
| --- | --- |
| Wrapping the append in a retry loop | Hides the collision, doubles the write cost, still fails under load |
| Disabling OPTIMIZE entirely | Small-file problem returns within days |
| `spark.databricks.delta.retryWriteConflict` style flags | Not a substitute for a disjoint read set |

Partition-scoped compaction is a one-line change and it removes the class of failure
rather than the symptom.
