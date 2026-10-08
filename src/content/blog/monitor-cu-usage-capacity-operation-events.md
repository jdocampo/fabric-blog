---
title: 'Measure Fabric CU usage with Capacity Operation Events'
description: 'Capture Fabric capacity events and use reusable KQL to measure notebook, Custom Live Pool, and pipeline CU consumption.'
pubDate: 2026-10-08
tags: ['fabric', 'spark', 'monitoring', 'data-engineering']
draft: false
---

# Measure Fabric CU usage with Capacity Operation Events

A faster pipeline is not necessarily a cheaper one. In [v0001's benchmark][benchmark], Bronze, Silver, and Gold notebooks consumed CU, but preparing a Custom Live Pool (CLP) also consumed CU under its **environment**. Pipeline orchestration was a separate component.

Capacity Operation Events let you measure those components. Here is a portal-first setup and reusable KQL, followed by one selected **CLP-HC** run: a Custom Live Pool with High Concurrency sharing one Spark session across three notebook activities.

## 1. Capture events before running the workload

You need **capacity Admin** access to the monitored capacity and **Contributor or higher** access to the workspace containing the Eventstream and Eventhouse.

1. Create an **Eventhouse** with a KQL database in your monitoring workspace.
2. Open **Real-Time hub → Fabric events → Capacity operation events → Create Eventstream**.
3. Select your capacity as the event scope, choose the destination workspace, and connect.
4. In the Eventstream's **Edit** mode, add an **Eventhouse** destination using **Direct ingestion**. Save, connect it to the stream, and **Publish**.
5. In **Live view**, select **Configure** on that destination. In the **Get data** wizard, create a table named `CapacityOperations`, inspect the JSON, and finish the mapping.

Preserve the CloudEvent envelope, especially `id`, `type`, and `time`, plus the nested **`data` column as `dynamic`**. Do not retain only the payload and discard the event ID.

Open a **KQL Queryset**, connect it to that database, and run these blocks separately:

```kql
.show table CapacityOperations schema as json
```

```kql
CapacityOperations
| where todatetime(['time']) > ago(1h)
| project EventTime = todatetime(['time']), type, id, data
| take 5
```

Confirm events reach the table before starting your measured workload. Set retention long enough for your analysis. Monitoring itself uses capacity; the queries below exclude its items from the workload total. If connectivity blocks capture, check the [official setup and network guidance][capture].

## 2. Record the IDs that own the consumption

Save workspace and capacity IDs, then the following item/operation pairs for each run:

| Component | Item ID | Billing `operationId` to match |
| --- | --- | --- |
| Notebook | Notebook item | Livy session ID from Spark monitoring/session details |
| CLP preparation | Environment item | Owned activation job ID, monitored with job type `SparkPool` |
| Pipeline, optional | Pipeline item | Each notebook activity's `activityRunId`, from pipeline monitoring |

For notebook activities, activity output exposes the session ID in `result.metadata.sessionId`; confirm it against Spark session details. [v0002][api-howto] shows how to find and monitor owned CLP activation jobs.

With HC, three activities can share **one session**: count its consumption once. Without HC, record all three distinct sessions. The event payload's `activationId` identifies a **capacity activation instance**, not your CLP activation job.

## 3. Calculate observed CU

Replace the placeholders and dates in this **common header**. The window must cover CLP preparation, session closure, and later events, not just pipeline execution. `time` is the CloudEvent timestamp; it is not the notebook start or an ingestion timestamp.

Paste this header followed immediately by **one** ending below, with no blank line between them, and run the combined query.

```kql
let From = datetime(2026-10-07T00:00:00Z);
let Until = datetime(2026-10-09T00:00:00Z);
let Raw = materialize(
    CapacityOperations
    | where todatetime(['time']) between (From .. Until)
    | where type == "Microsoft.Fabric.CapacityOperationEvents.Operation"
    | where tostring(data.workspaceId) == "<workspace-id>"
        and tostring(data.capacityId) == "<capacity-id>"
    | where tostring(data.itemId) in (
        "<notebook-item-id>", "<environment-item-id>", "<pipeline-item-id>")
    | project id, data
);
let Events = Raw
    | summarize take_any(data) by id
    | extend ItemId = tostring(data.itemId),
        OperationId = tostring(data.operationId),
        Kind = tostring(data.itemKind),
        Operation = tostring(data.operationName),
        CUms = todouble(data.capacityUnitMs),
        Status = tostring(data.status);
```

### First, check duplicate deliveries

Use this ending **before calculating totals**:

```kql
Raw
| summarize Payloads = make_set(tostring(data), 2),
    InvalidCU = countif(isnull(todouble(data.capacityUnitMs))) by id
| where isempty(id) or array_length(Payloads) > 1 or InvalidCU > 0
```

It should return no rows. If it reports missing IDs, invalid CU, or conflicting payloads for one ID, investigate before trusting totals. An empty source also passes this check, so verify capture separately. Once duplicate payloads agree, `take_any(data)` keeps one copy per **event ID**, not one per operation.

### Notebook sessions

```kql
Events
| where Kind == "SynapseNotebook" and ItemId == "<notebook-item-id>"
| where OperationId in ("<livy-session-id>")
| summarize CUms = sum(CUms), Events = count(),
    Statuses = make_set(Status) by ItemId, OperationId
| extend CUSeconds = CUms / 1000.0, CUHours = CUms / 3600000.0
```

For isolated notebooks, add their session IDs to the `in` list and sum the returned session totals. If they use different notebook items, add those item IDs to both the header and this filter.

The observed Spark events contain incremental consumption in progress and terminal records. **Sum both**, rather than keeping only the final record or filtering for `Success`. `todouble` preserves fractional CU-ms that `tolong` would truncate.

### CLP environment

```kql
Events
| where Kind == "Environment" and ItemId == "<environment-item-id>"
| where OperationId == "<sparkpool-activation-job-id>"
| summarize CUms = sum(CUms), Events = count(),
    Statuses = make_set(Status) by ItemId, OperationId, Operation
| extend CUSeconds = CUms / 1000.0, CUHours = CUms / 3600000.0
```

This separates operations such as **Custom Pool Startup** and **Custom Pool Ready**. Add their totals for the owned activation. Do not multiply that sum by the configured number of warm clusters; their reported consumption is already represented.

### Pipeline orchestration, optional

```kql
Events
| where Kind == "Pipeline" and ItemId == "<pipeline-item-id>"
| where OperationId in (
    "<bronze-activity-run-id>", "<silver-activity-run-id>",
    "<gold-activity-run-id>")
| summarize CUms = sum(CUms), Events = count(),
    Statuses = make_set(Status) by ItemId, OperationId
| extend CUSeconds = CUms / 1000.0, CUHours = CUms / 3600000.0
```

Keep orchestration separate from notebook execution. Check that every expected session, activation, or activity appears: **a missing row is unresolved, not zero**.

## 4. Read the result

For **CLP-HC-R4**, one selected run in the [benchmark resource][details], the exact reported components were:

| Component | Observed CU-seconds |
| --- | ---: |
| One shared notebook session | 7,201.8855 |
| CLP environment activation | 291.0935 |
| **Notebook + environment** | **7,492.9790** |
| Optional pipeline orchestration | 60.4800 |
| **Including orchestration** | **7,553.4590** |

The notebook-plus-environment subtotal is approximately **2.0814 CU-hours**: divide CU-seconds by 3,600. This is consumption, **not a monetary charge or a capacity-utilization percentage**. Round after summing, not per event.

Three notebook activities did not mean three copies of the shared-session charge. Equally, the environment activation was not free simply because the notebook acquired a ready cluster.

## Before calling the analysis complete

- **Wait for natural session closure and terminal events.** CU can continue after the last notebook finishes; that usage is already in the session total. Recheck after a later extraction for delayed delivery.
- **Keep the clocks separate.** In this benchmark, notebook `consumptionStartTime` behaved as a slice endpoint. Ingestion timestamps and smoothing windows are not execution time; do not multiply CU by a smoothing-window duration.
- **Report what is observed.** CLP Ready events did not establish complete waiting/idle coverage. An attached on-demand environment with no separate events remained unresolved, not a verified zero.

Use [v0001][benchmark] for the pool trade-offs and the [supporting resource][details] for timing, allocation, and replication detail. The repeatable method is simple: **capture events, record exact owners, deduplicate deliveries, sum reported consumption, and keep missing coverage visible**.

## References

- [v0001: Custom Live Pools and High Concurrency benchmark][benchmark], for the scenarios and results measured with this method.
- [Configure Capacity Operation Events in Real-Time hub][capture].
- [Capacity Operation Events schema][schema].
- [Add an Eventhouse destination to an Eventstream][destination].
- [Query data in a KQL Queryset][queryset].

[benchmark]: ../custom-live-pools-high-concurrency-medallion/
[api-howto]: ../manage-custom-live-pools-through-apis/
[details]: ../../resources/custom-live-pools-benchmark/
[capture]: https://learn.microsoft.com/en-us/fabric/real-time-hub/create-streams-fabric-capacity-operation-events
[schema]: https://learn.microsoft.com/en-us/fabric/real-time-hub/explore-fabric-capacity-operation-events
[destination]: https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/add-destination-kql-database
[queryset]: https://learn.microsoft.com/en-us/fabric/real-time-intelligence/kusto-query-set
