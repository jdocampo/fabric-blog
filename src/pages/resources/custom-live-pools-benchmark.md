---
layout: ../../layouts/Resource.astro
title: 'Custom Live Pools and High Concurrency: benchmark details and replication'
description: 'Selected-run results, notebook timing, cluster allocation, CU accounting, and the protocol behind the v0001 benchmark.'
---

This resource supports [v0001: Faster or cheaper?][article]. It documents the **three selected runs per scenario** used in that article: 18 pipelines, 54 notebook activities, 1,530 query executions, and 36 distinct billed notebook sessions. Measurements were captured on **7 October 2026**.

The same physical runs determine both latency and CU. Selection was based on pipeline latency, not on choosing the least expensive CU samples. This is a selected comparison, not a statistical guarantee of typical performance. Run labels preserve the original repetition number: `SP-R2`, for example, is the selected second repetition of SP.

The page contains results and replication instructions, not downloadable notebooks, templates, or raw logs. Workspace-specific identifiers are omitted. The recommendations remain in [the article][article]; this page explains what was measured and what can be inferred from it.

## Reading guide

- [Setup and workload](#setup-and-workload)
- [Compute and dependency settings](#compute-and-dependency-settings)
- [Selected results](#selected-results)
- [Notebook timing and charging](#notebook-timing-and-charging)
- [Cluster allocation and notebook CU](#cluster-allocation-and-notebook-cu)
- [Accounting and limitations](#accounting-and-limitations)
- [How to repeat the benchmark](#how-to-repeat-the-benchmark)
- [References](#references)

## Setup and workload

The workspace used an **F32 capacity in West Central US and managed private endpoints**. SP and SP-HC selected the Starter Pool configuration, but private connectivity required **on-demand fallback**, not the usual prewarmed Starter Pool startup within seconds. The experiment evaluates CLP where that fast Starter path is unavailable.

| Scenario | Selected compute path | Pipeline HC | Notebook applications per run | Maximum hydrated clusters |
| --- | --- | --- | ---: | --- |
| SP | Starter Pool, on-demand fallback; no environment | Off | 3 | N/A |
| OD | On-demand custom pool, published environment | Off | 3 | N/A |
| CLP | Custom Live Pool, published environment | Off | 3 | 3 |
| SP-HC | Starter Pool, on-demand fallback; no environment | On | 1 | N/A |
| OD-HC | On-demand custom pool, published environment | On | 1 | N/A |
| CLP-HC | Custom Live Pool, published environment | On | 1 | 1 |

Each pipeline ran **Bronze, then Silver, then Gold**, with no parallel notebook activities. These are stage labels for different TPC-DS query selections, not a claim that the queries reproduce every transformation in a production medallion pipeline.

The workload used **LakeBench 1.2.0 and TPC-DS SF100**. The same source snapshot and query order were used throughout. Every successful run completed 29 Bronze queries, 28 Silver queries, and 28 Gold queries.

### Exact query order

These are the LakeBench query identifiers, including the `a`/`b` variants. Do not substitute another query list or sort the identifiers.

```json
{
  "Bronze": ["q13", "q16", "q27", "q28", "q29", "q30", "q37", "q38", "q39b", "q40", "q42", "q43", "q44", "q51", "q53", "q55", "q57", "q58", "q61", "q62", "q63", "q65", "q68", "q69", "q81", "q85", "q88", "q90", "q95"],
  "Silver": ["q8", "q9", "q21", "q24b", "q26", "q32", "q39a", "q46", "q48", "q49", "q54", "q56", "q59", "q60", "q66", "q73", "q76", "q77", "q78", "q79", "q84", "q87", "q89", "q92", "q94", "q96", "q97", "q99"],
  "Gold": ["q12", "q15", "q17", "q18", "q19", "q20", "q22", "q24a", "q25", "q31", "q33", "q34", "q35", "q36", "q41", "q45", "q47", "q50", "q52", "q70", "q71", "q80", "q82", "q83", "q86", "q91", "q93", "q98"]
}
```

Queries used already prepared tables; data generation was not part of pipeline latency or notebook query execution. Validation confirmed that all 24 queried tables retained the recorded Delta versions and row counts. The source inventory contained 95 Parquet files totaling 31,453,187,652 bytes. The inventory also included its own manifest: its aggregate byte count was not used as a stable fingerprint because it did not reconcile. Actual table versions, counts, and file inventory were the controls.

Each notebook finished when its real work finished. There was **no target execution time, padding, fixed-rate CU replacement, or normalization**.

## Compute and dependency settings

These are configured limits, not the active cluster size at every instant.

| Setting | Recorded value |
| --- | --- |
| Published environment runtime | 2.0 |
| Starter and custom pool node family / size | MemoryOptimized / Medium |
| Pool node autoscale | Enabled, minimum 1 / maximum 10 |
| Configured driver | 8 vCores, 56g memory |
| Configured executor | 8 vCores, 56g memory |
| Dynamic executor allocation | Enabled, minimum 1 / maximum 9 |
| Notebook session keep-alive setting | 0 |
| Notebook Spark session timeout | `spark.synapse.nbs.session.timeout=1200000` |
| CLP and CLP-HC cluster idle timeout | `PT50M` |
| CLP and CLP-HC Live Pool lifespan | `PT2H` |

CLP and CLP-HC used separate published environments so their maximum hydrated-cluster counts could be three and one respectively. OD and OD-HC used an environment with Custom Live Pool support disabled.

### Acceleration and efficient scaledown

The published environments used the following properties. NEE and RSM were also required and verified in the Starter notebook configuration; selecting a pool alone was not considered proof that acceleration was active.

| Spark property | Value |
| --- | --- |
| `spark.native.enabled` | `true` |
| `spark.remote.shuffle.enabled` | `true` |
| `spark.sql.rsm.decisionlayer.enabled.level` | `stage` |
| `spark.sql.adaptive.enabled` | `true` |
| `spark.sql.adaptive.shuffleWrite.enabled` | `true` |
| `spark.storage.decommission.shuffleBlocks.enabled` | `true` |
| `spark.storage.decommission.shuffleBlocks.cleanup` | `true` |
| `spark.storage.decommission.shuffleBlocks.migrateToFallbackStorage` | `true` |
| `spark.storage.decommission.fallbackStorage.cleanUp` | `true` |
| `spark.dynamicAllocation.preventShutdownExecutorWithCache` | `false` |
| `spark.dynamicAllocation.excludeDeltaSnapshotCache` | `true` |

Each stage checked its effective properties and ran a small native-execution probe before the timed query batch. Environment publication and compute readback were verified before submission.

### Libraries and initialization

Environment-backed notebooks used published `lakebench==1.2.0`. Starter notebooks carried pinned wheels, unpacked them locally into a temporary site directory, and used a completion marker to avoid repeated extraction. That bootstrap did **not** download and resolve packages through a fresh network installation or restart Python.

There were no dedicated bootstrap start/end timers. The CU before the first preflight marker includes initialization, imports, dependency preparation, and bookkeeping; it is not an independently measured library-installation cost. These results cannot establish a universal Starter library surcharge or predict the cost of a fresh network package installation.

## Selected results

All CU values below are **observed consumption**, not proof of complete lifecycle coverage. All times are seconds. Aggregate rows are means of the selected three complete pipeline runs, not per-notebook averages.

### Scenario means

| Scenario | Pipeline latency s | Selected latency range s | Notebook CU-s | Environment CU-s | Observed notebook + environment CU-s |
| --- | ---: | --- | ---: | --- | ---: |
| SP | 1165.972 | 1128.695-1186.454 | 7542.060 | N/A | 7542.060 |
| OD | 1256.899 | 1214.330-1280.699 | 8085.359 | Unresolved | 8085.359 |
| CLP | 844.230 | 821.485-878.576 | 8669.276 | 678.948 | 9348.224 |
| SP-HC | 744.350 | 711.255-783.938 | 7528.742 | N/A | 7528.742 |
| OD-HC | 731.734 | 723.054-736.233 | 7819.711 | Unresolved | 7819.711 |
| CLP-HC | 571.869 | 554.411-591.207 | 7221.952 | 227.195 | 7449.148 |

The article presents these same measurements at lower precision. Figures here are rounded to three decimal places from the original values, so adding displayed components may differ slightly from the displayed subtotal.

### Individual selected runs

| Run | Pipeline latency s | Notebook CU-s | Environment CU-s | Observed notebook + environment CU-s | Separate pipeline CU-s |
| --- | ---: | ---: | --- | ---: | ---: |
| SP-R2 | 1186.454 | 7869.912 | N/A | 7869.912 | 60.480 |
| SP-R3 | 1128.695 | 7216.796 | N/A | 7216.796 | 60.480 |
| SP-R4 | 1182.766 | 7539.472 | N/A | 7539.472 | 60.480 |
| OD-R2 | 1280.699 | 8209.630 | Unresolved | 8209.630 | 60.480 |
| OD-R3 | 1275.669 | 8187.454 | Unresolved | 8187.454 | 60.480 |
| OD-R4 | 1214.330 | 7858.992 | Unresolved | 7858.992 | 60.480 |
| CLP-R2 | 832.628 | 8289.559 | 635.078 | 8924.637 | 60.480 |
| CLP-R3 | 878.576 | 8998.016 | 639.552 | 9637.568 | 60.480 |
| CLP-R4 | 821.485 | 8720.254 | 762.215 | 9482.468 | 60.480 |
| SP-HC-R2 | 711.255 | 7282.508 | N/A | 7282.508 | 60.480 |
| SP-HC-R3 | 737.856 | 7628.186 | N/A | 7628.186 | 60.480 |
| SP-HC-R4 | 783.938 | 7675.533 | N/A | 7675.533 | 60.480 |
| OD-HC-R2 | 723.054 | 7805.518 | Unresolved | 7805.518 | 60.480 |
| OD-HC-R3 | 736.233 | 7829.048 | Unresolved | 7829.048 | 60.480 |
| OD-HC-R4 | 735.913 | 7824.567 | Unresolved | 7824.567 | 60.480 |
| CLP-HC-R1 | 554.411 | 7272.879 | 215.092 | 7487.970 | 60.480 |
| CLP-HC-R3 | 569.988 | 7191.092 | 175.401 | 7366.493 | 60.480 |
| CLP-HC-R4 | 591.207 | 7201.886 | 291.094 | 7492.979 | 60.480 |

Pipeline orchestration CU is separate from the notebook-plus-environment subtotal. An HC notebook session is counted once, even though three activities use it.

**N/A** means no attached environment. **Unresolved** means an environment was attached but no separate environment billing events were observed; it does not mean zero environment consumption. For CLP paths, the subtotal includes reported activation consumption, but Ready reporting does not prove that every waiting or idle interval is covered.

## Notebook timing and charging

Different clocks answer different questions:

| Clock | Source and meaning |
| --- | --- |
| Pipeline start/end | Service job timestamps; used for end-to-end pipeline latency |
| Notebook execution start/end | Activity output `result.metadata.runStartTime` / `runEndTime` |
| Query batch start/end | Workload markers around the ordered LakeBench query batch |
| Spark session start/end | Livy `startDateTime` / `endDateTime` |
| Observed first charge interval start | Billing `operationStartTime` |
| Charge interval endpoint | Notebook billing `consumptionStartTime`, checked against duration and preceding endpoint |
| Event arrival | `EventProcessedUtcTime`; ingestion time, not execution or charging start |

The name `consumptionStartTime` is misleading for the observed notebook slices: it behaves as an **interval endpoint**, not the beginning of the charge. Do not use the first received event or its ingestion timestamp as the time charging began.

### Selected session timing ranges

Each range is across selected notebook sessions: nine sessions for each isolated scenario, or three shared sessions for each HC scenario. For a shared session, notebook start is the first attached workload's start and notebook end is the last workload's end.

| Scenario | Charging begins after notebook start s | Charging begins after Spark session start s | Charging ends after last notebook end s | Charge endpoint minus Spark session end s |
| --- | --- | --- | --- | --- |
| SP | 103.172-123.150 | 33.870-35.630 | 3.016-7.413 | 0.151-0.952 |
| OD | 118.032-142.972 | 53.830-60.049 | 2.781-8.890 | 0.060-0.979 |
| CLP | 5.092-7.345 | 212.351-857.069 | 1.905-8.953 | 0.030-0.804 |
| SP-HC | 104.158-173.984 | 34.325-58.294 | 80.965-119.882 | 0.303-0.847 |
| OD-HC | 122.947-137.190 | 54.894-60.803 | 118.559-123.042 | 0.026-0.708 |
| CLP-HC | 5.412-7.110 | 252.994-285.211 | 75.119-115.104 | 0.535-0.855 |

CLP sessions existed before pipeline submission, so their Spark session start is not the notebook execution start. The delay from that earlier session start includes precreated capacity waiting for acquisition; notebook and environment accounting must remain separate.

Charging ended within one second of the recorded Spark session end in all 36 selected sessions. It did not necessarily end when the last query or notebook finished. In shared sessions, CU can also be consumed between notebook activities.

### Representative selected sessions

All times below are **UTC on 7 October 2026**, displayed to millisecond precision with finer digits omitted. For isolated scenarios, these examples show only the Bronze session, not the complete pipeline. For HC scenarios, they show the one application serving all three activities.

| Run / stages | First notebook start | Spark session start | Charging begins | Last query batch ends | Last notebook ends | Charging ends |
| --- | --- | --- | --- | --- | --- | --- |
| SP-R4 / Bronze | 21:55:44.509 | 21:56:54.000 | 21:57:28.858 | 22:01:30.538 | 22:01:37.564 | 22:01:42.150 |
| OD-R4 / Bronze | 21:33:03.830 | 21:34:12.000 | 21:35:11.556 | 21:39:29.081 | 21:39:34.315 | 21:39:39.354 |
| CLP-R4 / Bronze | 21:17:01.972 | 21:13:15.000 | 21:17:07.856 | 21:21:04.354 | 21:21:10.471 | 21:21:12.487 |
| SP-HC-R4 / Bronze, Silver, Gold | 20:19:01.310 | 20:20:57.000 | 20:21:55.294 | 20:31:24.759 | 20:31:29.337 | 20:32:50.303 |
| OD-HC-R4 / Bronze, Silver, Gold | 20:54:18.613 | 20:55:35.000 | 20:56:35.803 | 21:06:05.997 | 21:06:09.467 | 21:08:08.025 |
| CLP-HC-R4 / Bronze, Silver, Gold | 20:42:04.206 | 20:37:26.000 | 20:42:11.211 | 20:51:20.005 | 20:51:23.430 | 20:52:38.548 |

## Cluster allocation and notebook CU

CU measures allocated compute over time, not simply CPU utilization or pipeline wall-clock duration.

| Scenario | Mean summed billed seconds | Weighted billed-equivalent vCores | Peak billed-equivalent vCores | Mean query batch seconds | Mean CU-s after notebook completion, approximate |
| --- | ---: | ---: | ---: | ---: | ---: |
| SP | 787.329 | 19.159 | 24 | 657.553 | 175.735 |
| OD | 798.644 | 20.248 | 24 | 644.727 | 210.728 |
| CLP | 789.820 | 21.953 | 32 | 677.255 | 188.031 |
| SP-HC | 668.647 | 22.519 | 24 | 440.955 | 1170.589 |
| OD-HC | 694.504 | 22.519 | 24 | 441.437 | 1441.608 |
| CLP-HC | 632.693 | 22.829 | 24 | 424.965 | 1167.108 |

For SP/OD/CLP, billed duration and query time sum three distinct sessions; they are not pipeline elapsed time. The last column sums consumption after each stage's notebook completes. For HC, it represents consumption after the final Gold notebook completes. It is already part of notebook CU, not an extra amount to add to the total.

Approximate CU after notebook completion represented about **15.5-18.4% of observed notebook CU for HC**, compared with about **2.2-2.6% for isolated sessions**. Phase allocations assume a uniform average rate within each validated billing slice; they are not independently metered phases.

### What "cluster size" means here

The driver and first executor shared a physical host in all 36 selected sessions. Their configured 8 vCores must not be added as if they were two separate hosts. Executor registration history was grouped by host to avoid double counting.

Most observed billing intervals were near 4 CU per second, equivalent to 8 billed vCores, or 12 CU per second, equivalent to 24 billed vCores. The selected sessions each reached three registered physical hosts. A driver-host executor being removed does not prove the host was released: the driver can remain there. Likewise, registered executor cores can fall while a larger physical or billed footprint remains allocated.

The weighted billed-equivalent footprint is:

```text
weighted billed-equivalent vCores
  = 2 * total notebook CU-seconds / summed reported billed seconds
```

This is inferred from metering using 1 CU = 2 Spark vCores; it is **not** a complete VM history or a measurement of CPU utilization. For a scenario, the ratio uses totals across its selected runs rather than averaging per-run ratios.

### Autoscaling, reuse, and dependencies

Within SP/OD/CLP and within SP-HC/OD-HC/CLP-HC, scale-up happened at different times. The resulting differences in time spent at larger allocations are consistent with expected autoscaling variation, not a fixed surcharge for a named pool type.

HC reused an expanded cluster between activities instead of starting every stage with a new, initially smaller application. For SP versus SP-HC, mean summed billed time fell from 787.329 to 668.647 seconds, while weighted billed-equivalent vCores rose from 19.159 to 22.519. Those effects nearly cancel: mean notebook CU changed by about -0.177%, despite the much shorter pipeline latency.

Cold acquisition and context preparation largely preceded the observed notebook charging start. Removing those waits can therefore shorten a pipeline without removing an equal amount of notebook CU. CLP environment consumption still needs to be considered separately.

A fixed-size cluster could reduce scale-up timing variation and make notebook CU more comparable. This test deliberately retained autoscaling to resemble real-world operation; fixed size would still not eliminate differences in query duration, dependencies, or session lifetime.

## Accounting and limitations

For portal setup and reusable KQL, see [Measure Fabric CU usage with Capacity Operation Events][cu-monitoring]. The rules below explain how those measurements were applied to this benchmark.

### Operation ownership and summation

Notebook charges were matched using the workspace, capacity, notebook item, and exact **Livy session operation ID**. A notebook activity ID is not substituted for the session ID. HC's shared session was counted once, not once per attached notebook.

Environment charges were matched to the owned **SparkPool activation job ID**. The billing payload's `activationId` is a different identity and was not used as a substitute. CLP's three warm clusters were represented by distinct events under their activation; the activation sum was not multiplied by three again.

Pipeline activity charges were matched to recorded activity run IDs. The three activities consumed 60.480 CU-s per selected pipeline run, reported separately.

CloudEvent deliveries were deduplicated by root event `id`, keeping the latest processed copy and rejecting conflicting payloads. Incremental nonzero slices were summed, including both progress and terminal records. Keeping only the final record would discard earlier usage.

```text
CU-seconds = sum(deduplicated, exactly attributed capacityUnitMs) / 1,000
CU-hours   = sum(deduplicated, exactly attributed capacityUnitMs) / 3,600,000
```

Decimal arithmetic preserved fractional CU-ms. Rounding was applied only for presentation. Events were followed through session closure rather than clipped at pipeline end. Terminal evidence and spaced extraction snapshots were checked for late nonzero increments.

### Missing coverage is not zero

| Component | Interpretation |
| --- | --- |
| SP/SP-HC environment | No attachment; not applicable |
| OD/OD-HC environment | No separate events observed; unresolved, not verified zero |
| CLP/CLP-HC environment | Reported Startup/Ready events included; Ready durations do not establish the full observed waiting period |
| Notebook totals | Exactly summed reported CU with terminal evidence; not proof that every wall-clock interval was metered |
| Pipeline | Separately observed orchestration CU, excluded from notebook/environment subtotal |

The three-minute wait after observed readiness was a submission control, not an assumed charge calculated as three minutes times a fixed CU rate. Missing or unexplained coverage was not filled with estimated usage.

### Selected timing and allocation discrepancies

**SP-HC-R3:** one notebook slice reports 60.188 seconds of duration, but consecutive endpoints differ by 85.276954 seconds: a 25.088954-second mismatch. Its **722.264 CU-s remains in notebook totals**, but is not allocated to workload, initialization, or after-notebook phases. Do not invent a charge for the apparent gap. The mean unallocated phase consumption across SP-HC's selected runs is 240.755 CU-s.

**CLP-R4:** a 61.646-second slice is near 16 CU per second, equivalent to 32 billed vCores, while captured registration history shows at most three hosts. This is an unresolved allocation/monitoring discrepancy. Allocated-but-not-registered resources or metering semantics are possible explanations, not verified additional physical nodes.

**Initialization:** mean CU before the first preflight marker was 240.271 for SP, 330.863 for OD, 259.941 for CLP, 161.857 for SP-HC, 130.549 for OD-HC, and 105.986 for CLP-HC. These approximate envelopes include more than library preparation; do not relabel them as library-only charges.

The session collectors waited for natural completion and did not cancel or explicitly stop the measured sessions. CU after notebooks finish is therefore not an artifact of a collector-driven shutdown.

## How to repeat the benchmark

These instructions reproduce the **protocol**, not a one-click release of the private runner or a guarantee of identical numbers. Use your own workspace and item identifiers. Fabric runtime, workload data, scaling decisions, capacity competition, and service behavior can change the results.

### 1. Establish the same scope

Use an isolated, paid F32 workspace with comparable private-connectivity requirements. Without managed private endpoints or other settings that require dedicated provisioning, Starter Pool sessions may use the seconds-fast prewarmed path; that is a different experiment. HC can still be useful there, but CLP no longer removes the same cold-start delay.

Confirm workspace Admin access for configuration changes, the assigned capacity, and the absence of competing workloads. Record the initial Spark settings so they can be restored. Avoid running two trials in parallel when they require different workspace-wide pipeline HC settings.

### 2. Prepare and freeze SF100 data

Follow the [LakeBench TPC-DS workflow][lakebench] to prepare SF100 data and query tables in a lakehouse. Use the same snapshot for all trials, recording each queried table's Delta version and row count, plus the source-file inventory. Validate them again after the tests.

Regenerating SF100 data alone does not prove it matches the source used here. Keep the generator/version/seed and physical table preparation consistent within your own experiment; report any differences rather than claiming byte-identical replication from the scale factor.

Data generation, dependency downloads, and environment publication must finish before timed pipeline submission.

### 3. Prepare the compute paths

Configure the Starter selection and a MemoryOptimized Medium custom pool with the recorded autoscale bounds. Create the published on-demand environment and separate CLP/CLP-HC environments; use the library pin and compute properties above.

Use [v0002's REST API workflow][api-howto] for pool/environment configuration, publication, activation scheduling, and readiness monitoring. Read back published settings instead of relying on staged values or an unchanged display name. Record publication versions.

For the Starter selections, do not attach the custom environment. The observed bootstrap uses locally embedded pinned wheels and a completion-marker cache; replacing that with a network install changes the experiment. Verify the effective notebook Spark settings, including the acceleration properties.

### 4. Build the three-activity pipeline

Use three dependent notebook activities in Bronze/Silver/Gold order, all under the same identity, workspace, and default lakehouse. Parameterize the stage and use its exact ordered query list above. Keep table locations, query selections, and result validation identical across scenarios.

Configure LakeBench's TPC-DS benchmark for query execution against the prepared source, with scale factor 100, the stage query list, and a unique benchmark run ID. Run the query batch once per activity; do not include data generation or add a sleep to reach a target duration.

The measured runner disabled LakeBench's modeled dollar-price lookup and cost estimates; CU came from Capacity Operation Events, not a retail-price model. It also adapted optional history-link metadata when a session's Spark UI URL lacked the GUID shape expected by LakeBench 1.2.0, restoring the original property after construction. These were compatibility adjustments, not query or timing normalization. If your pinned version needs such adjustments, document them; do not report constructor pricing placeholders as observed costs.

Record before/after query-batch UTC timestamps and a monotonic elapsed timer. Persist the ordered query list or its checksum, result count, failures, duplicates, effective settings, application identity, notebook identity, and environment identity. Verify native execution with a small preflight probe outside the timed query batch.

### 5. Control sharing and warm readiness

Turn pipeline HC **off for SP/OD/CLP**, and **on for SP-HC/OD-HC/CLP-HC**. Use one unique session tag per HC trial, shared by that trial's three activities; never reuse it to accidentally attach to an earlier run. Confirm one application for HC and three distinct applications for isolated sessions.

For CLP paths, monitor the owned activation with job type **SparkPool**. Record the first UTC observation with at least one ready cluster, then submit the pipeline **180 seconds after that observation**. This uses observed readiness, not a claim to know the exact instant a cluster became ready.

Verify the warm handoffs: CLP must acquire three intended precreated applications, while CLP-HC must use one shared application. Record the owned activation, readiness samples, actual submission time, acquisition state, and preexisting Spark application start. A missing warm handoff is not silently accepted as a CLP result.

### 6. Execute and validate

Run trials serially, waiting for each trial's pipeline and session lifecycle to finish before changing shared workspace settings or starting the next trial. Pipeline activities and all 85 queries must succeed. Check query order/count, missing or duplicate results, environment/pool routing, acceleration, and expected application sharing.

Prespecify the repetitions and any selection policy before examining comparative results. Use the **same selected physical runs for latency and CU**. Do not independently choose cheaper CU samples.

### 7. Collect lifecycle and CU evidence

Record pipeline, activity, notebook, environment publication, Livy session, Spark application, and owned activation identities privately. Capture notebook metadata, workload markers, session details, executor add/remove history, resource allocation samples, and available driver logs. Explicitly record unavailable monitoring data instead of treating it as absence of work.

Wait for natural session closure; do not stop a session simply because Gold completed. Match billing operations using the exact identities described above, sum deduplicated incremental events, and require terminal evidence. Take at least two extraction snapshots separated by two minutes and investigate new nonzero increments before finalizing totals.

Keep environment and orchestration consumption separate from notebook consumption. Do not clip charges at pipeline end, substitute fixed CU rates, or turn unresolved component coverage into zero. Configuration and querying of Capacity Operation Events are a separate monitoring topic; this page records the accounting rules used for the comparison.

### 8. Restore and interpret

Disable activation schedules owned by the experiment, confirm no sessions or pending pipeline jobs remain, and restore the original default pool and HC setting without overwriting unrelated changes.

Compare latency alongside the time-integrated allocation and CU used after notebooks finish. Report the selected-run sample size and coverage limits. Use the observations to evaluate your own service-level objective and capacity headroom, not to promise that one pool configuration is universally cheapest.

## References

- [v0001: recommendations and trade-offs][article].
- [v0002: Custom Live Pools REST API workflow][api-howto].
- [Microsoft LakeBench][lakebench].
- [Custom Live Pools overview][live-overview].
- [Custom Live Pools configuration][live-config].
- [High Concurrency for notebooks in pipelines][hc-pipelines].
- [Spark billing and utilization][spark-billing].

[article]: ../../blog/custom-live-pools-high-concurrency-medallion/
[api-howto]: ../../blog/manage-custom-live-pools-through-apis/
[cu-monitoring]: ../../blog/monitor-cu-usage-capacity-operation-events/
[lakebench]: https://github.com/microsoft/LakeBench
[live-overview]: https://learn.microsoft.com/en-us/fabric/data-engineering/custom-live-pools-overview
[live-config]: https://learn.microsoft.com/en-us/fabric/data-engineering/custom-live-pools-configure
[hc-pipelines]: https://learn.microsoft.com/en-us/fabric/data-engineering/configure-high-concurrency-session-notebooks-in-pipelines
[spark-billing]: https://learn.microsoft.com/en-us/fabric/data-engineering/billing-capacity-management-for-spark
