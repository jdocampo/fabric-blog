---
title: 'Refresh Materialized Lake Views with OneLake events: match the Delta commit, not the file noise'
description: 'Configure event-triggered MLV refresh in Microsoft Fabric with a UI walkthrough and OneLake event filters that match published Delta commits.'
pubDate: 2026-10-07
tags: ['fabric', 'onelake', 'data-engineering', 'materialized-lake-views']
draft: false
---

# Refresh Materialized Lake Views with OneLake events: match the Delta commit, not the file noise

A source table receives new data at an unpredictable time. A Materialized Lake View (MLV) transforms that table for downstream consumers. How do you refresh the view after the write, without polling on a fixed schedule or adding a refresh call to every ingestion notebook?

Microsoft Fabric supports **event-triggered MLV refresh using OneLake events**. The useful signal for a Delta table is the publication of a transaction-log commit, not every file operation associated with the write.

This article explains how to configure the trigger using generic names and a small example.

> **Availability:** Event-triggered MLV refresh is documented as **Preview** at publication time. Confirm availability in your environment. Private link support is outside the documented preview scope. ([Scheduling documentation][schedules])

## Two different jobs: starting a refresh and choosing its strategy

There are two mechanisms to keep separate:

| Mechanism | What it does |
| --- | --- |
| OneLake event-triggered refresh | Detects a matching event and starts the MLV refresh workflow. |
| Optimal refresh | Once a run starts, analyzes source Delta changes and chooses no refresh, incremental refresh, or full refresh. |

Turning on Optimal refresh does **not** by itself subscribe the view to source-table changes. Configure an event trigger to start the refresh when a matching source event arrives.

With the trigger configured, the sequence becomes: a normal Delta write publishes a commit; OneLake emits a matching file event; the managed action starts the selected MLV refresh; Fabric applies the appropriate refresh strategy.

CDF is a separate consideration. Enable Delta change data feed on source tables if you want incremental refresh eligibility, but do not treat it as an event subscription or a promise that every operation will refresh incrementally. ([Optimal refresh documentation][refresh])

## The signal: publication of the final Delta commit

A Delta writer can publish a commit by first writing a temporary file, then renaming it to the final versioned JSON:

```text
/Tables/dbo/source_records/_delta_log/.00000000000000000004.json.<unique-id>.tmp
  becomes
/Tables/dbo/source_records/_delta_log/00000000000000000004.json
```

For that write path, the relevant fields of the rename event look like this. This is a **partial event example**, not a complete CloudEvents payload:

```json
{
  "type": "Microsoft.Fabric.OneLake.FileRenamed",
  "subject": "/Tables/dbo/source_records/_delta_log/00000000000000000004.json",
  "data": {
    "api": "RenameFile"
  }
}
```

The event's `subject` identifies the **destination**, the final JSON commit path. In this rename payload, `sourceUrl` identifies the temporary file and `destinationUrl` the final file. The version number above is illustrative; it changes with each commit.

These fields answer different questions:

| Field | Matching principle |
| --- | --- |
| `type` | Which category of OneLake event occurred? |
| `subject` | Did the operation affect a final JSON file under the intended table's log path? |
| `data.api` | Was it a publication operation, rather than initial file creation? |

OneLake events describe **storage operations**, not SQL verbs. An INSERT, UPDATE, DELETE, or MERGE can match the same rule because each publishes a Delta commit. The trigger does not need a separate condition for each SQL statement. ([OneLake event schema][events])

## 1. Prepare a safe example

Use a dedicated demonstration workspace and unused object names. The names below are generic:

| Object | Example value |
| --- | --- |
| Workspace | `AnalyticsWorkspace` |
| Schema-enabled lakehouse | `DemoLakehouse` |
| Schema | `dbo` |
| Source Delta table | `dbo.source_records` |
| Materialized Lake View | `dbo.records_mlv` |
| Event-triggered configuration | `RefreshRecordsOnCommit` |

You need active Fabric capacity, an available Spark notebook, and permissions to create the source table/MLV and manage its refresh configuration. The subscription owner also needs **`SubscribeOneLakeEvents` on the source item**. Loss of that permission can pause event delivery. ([Subscription permissions][permissions])

Attach `DemoLakehouse` as the notebook's default lakehouse and verify that schemas are enabled. Run the following in Fabric Spark notebook cells, **not the lakehouse SQL analytics endpoint**. The two-part names refer to the default lakehouse; for another workspace/lakehouse, use the corresponding four-part Spark SQL names.

Create the source and baseline data:

```sql
%%sql
CREATE TABLE dbo.source_records (
    id BIGINT,
    marker STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

INSERT INTO dbo.source_records VALUES
    (1, 'record_a'),
    (2, 'record_b'),
    (3, 'record_c');
```

Create a SQL-defined MLV with a simple deterministic projection:

```sql
%%sql
CREATE MATERIALIZED LAKE VIEW dbo.records_mlv
AS
SELECT id, marker
FROM dbo.source_records;
```

This example deliberately avoids replacing an existing table or view. If the names already exist, choose unused names and substitute them consistently in the SQL, event paths, and refresh scope. ([MLV Spark SQL reference][create])

Never edit commit JSON or upload arbitrary files into `_delta_log`. Let normal Delta operations manage the transaction log.

## 2. Create the event-triggered configuration

Configure the trigger **from the lakehouse**, rather than manually editing its generated Activator or refresh notebook.

1. Open `DemoLakehouse`, select **Materialized lake views**, then **Manage**.
2. In the lineage toolbar, select **Manage schedules**.
3. Select **New schedule**, enter `RefreshRecordsOnCommit`, and optionally add a description.
4. Choose **Refresh selected materialized lake view(s)** and select only `dbo.records_mlv`. Confirm the highlighted scope in the lineage view.
5. Under **Refresh type**, choose **Event-triggered**.
6. Set **Event source type** to **OneLake events** and select the source lakehouse, `DemoLakehouse`.
7. Select the source table's OneLake log path:

```text
/Tables/dbo/source_records/_delta_log
```

Subscribe to both event types:

```text
Microsoft.Fabric.OneLake.FileCreated
Microsoft.Fabric.OneLake.FileRenamed
```

## 3. Set filters for final commit publication

Use **Set filters** to apply the following conditions:

| Field | Operator | Value |
| --- | --- | --- |
| `subject` | String begins with | `/Tables/dbo/source_records/_delta_log` |
| `subject` | String ends with | `.json` |
| `subject` | String not begins with | `/Tables/dbo/source_records/_delta_log/_stats/` |
| `data.api` | String in | `FlushWithClose`, `RenameFile` |

Selecting the OneLake source path supplies the first prefix condition. Add the remaining conditions through **Set filters** and ensure all four are present.

The filter rows are combined with **AND**. The two `data.api` values are alternatives within that one condition: **FlushWithClose OR RenameFile**. The selected event types are alternatives too.

Each part has a purpose:

| Condition | Why it matters |
| --- | --- |
| Source-log prefix | Limits the subscription to the intended source, excluding unrelated tables and the MLV's output path. |
| Final `.json` suffix | Rejects `.tmp`, `.crc`, Parquet statistics, and `_last_stats`. A temporary name containing `.json` still fails because it ends in `.tmp`. |
| `_stats/` exclusion | Defensively rejects JSON files under the statistics subtree if an implementation creates them there. |
| `RenameFile` API | Accepts the observed atomic publication of a temporary commit as its final JSON name. |
| `FlushWithClose` API | Allows a completed write to a final JSON path, while excluding its initial `CreateFile` operation. |

The `_stats` directory contains internal statistics files, such as Parquet files under `_raw_v1` and `_latest_v1`, temporary statistics files, and the `_last_stats` marker. These are not the versioned JSON transaction-log commits that should start the refresh.

Consider the matching results for individual events:

| Example | Matches? |
| --- | --- |
| Temporary `.json.<unique-id>.tmp`, `FlushWithClose` | No: wrong suffix. |
| Final versioned `.json`, `CreateFile` | No: wrong API operation. |
| Final versioned `.json`, `RenameFile` | Yes, with `FileRenamed` subscribed. |
| Final versioned `.json`, `FlushWithClose` | Yes, with `FileCreated` subscribed. |
| JSON under `_stats/` | No: excluded subtree. |
| Commit under another table or the MLV output | No: wrong source path. |

`FileRenamed` / `RenameFile` covers publication by rename. `FileCreated` / `FlushWithClose` allows writers that complete a write directly to the final JSON path. Writer behavior can differ, so choose the event types and API values appropriate to your ingestion path.

## 4. Save and confirm the configuration

Under **Advanced settings**, keep **Optimal refresh** on. Use an accessible Spark environment appropriate for the workload; preserve existing environment and resource settings unless you intend to change them.

Select **Save**, ensure the configuration is **On**, and reopen it to confirm the selected MLV, source path, event types, and persisted filters.

Fabric creates managed **FMLV Refresh notebook** and **Activator** items for event-triggered refresh. You can inspect the Activator's source events and activation history for diagnosis, but **do not modify or delete these generated items**. Make configuration changes through the lakehouse editor. ([Managed-item guidance][schedules])

## Production considerations

The final JSON suffix is a practical filter, **not a full Delta filename validator**. These UI conditions do not enforce a regular expression such as `[0-9]{20}[.]json`; another JSON file under the watched path could match.

A Delta commit also does not necessarily represent a logical row change. Maintenance or table-property operations can publish commits. The subscription matches file publication, not the transaction's contents.

For update/delete incremental processing, CDF alone is insufficient: the MLV needs supported row-identity refresh hints and a compatible definition. Fabric can otherwise choose full refresh, including when a small dataset makes recomputation cheaper. ([Refresh strategies][refresh])

Event-driven does not mean instantaneous, free, or exactly once. Compute availability and refresh execution still determine freshness and cost. Fabric can skip a later refresh while another is in progress, so account for overlapping commits and refreshes when designing for bursty workloads. ([Scheduling behavior][schedules])

**The useful design rule is to match source commit publication and keep source and output paths isolated.** A narrowly filtered event trigger reduces avoidable requests; it does not replace production freshness monitoring.

## References

- [Schedule a Materialized Lake View Refresh][schedules]
- [Explore OneLake events in Fabric Real-Time hub][events]
- [Subscribe permission for Azure and Fabric events][permissions]
- [Refresh Materialized Lake Views in a Lakehouse][refresh]
- [Spark SQL Reference for Materialized Lake Views][create]

[schedules]: https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/schedule-lineage-run
[events]: https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-onelake-events
[permissions]: https://learn.microsoft.com/en-us/fabric/real-time-hub/fabric-events-subscribe-permission
[refresh]: https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view
[create]: https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/create-materialized-lake-view
