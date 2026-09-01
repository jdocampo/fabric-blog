---
title: 'V-Order is not free'
description: 'Fabric writes V-Ordered Parquet by default. That is right for gold tables and wrong for high-churn bronze ingestion.'
pubDate: 2026-05-02
tags: ['fabric', 'spark', 'delta-lake']
---

V-Order is a write-time optimisation applied to Parquet files in Fabric: sorting, row
group sizing and encoding tuned so the engines behind Direct Lake can read the file
faster. Read wins are real. The cost lands on the writer, and it is not small.

## Where it pays

Gold tables. Written once per batch, read constantly by semantic models and SQL
endpoints. The write happens on your schedule; the reads happen in front of a user
waiting for a report. Trade write time for read time every time.

## Where it does not

Bronze ingestion — high volume, append-only, read almost exclusively by the next Spark
job in the chain. Nothing downstream benefits from the sort, and you have paid for it on
every single write.

Turn it off at session level for those notebooks:

```python
spark.conf.set("spark.sql.parquet.vorder.default", "false")
```

Or per table, so the setting travels with the data rather than the notebook:

```sql
ALTER TABLE bronze.raw_events
SET TBLPROPERTIES ('delta.parquet.vorder.default' = 'false');
```

## Measure before you decide

Do not take my numbers. Write the same batch twice into two tables and compare:

```python
import time

def timed_write(df, name, vorder):
    spark.conf.set("spark.sql.parquet.vorder.default", str(vorder).lower())
    start = time.time()
    df.write.mode("overwrite").saveAsTable(name)
    return round(time.time() - start, 1)

print("vorder on :", timed_write(df, "bench.with_vorder", True))
print("vorder off:", timed_write(df, "bench.no_vorder", False))
```

The gap scales with column count and cardinality, so a wide, messy bronze table is
exactly where it hurts most — and exactly where the read benefit is worth least.
