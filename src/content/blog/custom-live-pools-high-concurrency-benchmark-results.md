---
title: 'Benchmark data: Custom Live Pools and High Concurrency'
description: 'Raw Fabric pipeline timings, Spark application counts, environment consumption, and CU attribution for the S-A-B-C-D benchmark.'
pubDate: 2026-09-01
tags: ['fabric', 'spark', 'benchmark', 'performance']
draft: true
---

This document contains the raw measurements, CU-attribution method, summary tables, and analysis used by the companion article, [Faster or cheaper? Custom Live Pools and High Concurrency for Fabric medallion ETL](/blog/custom-live-pools-high-concurrency-medallion/).

## Scope and controls

The core lab used a simplified sequential medallion pipeline:

```text
Bronze -> Silver -> Gold
```

Each run processed the same deterministic 20,000,000-row synthetic workload and wrote equivalent Bronze, Silver, and Gold Delta outputs.

| Control | Value |
| --- | --- |
| Fabric capacity | F32, provisioned-capacity model |
| Region | West Central US |
| Fabric runtime | 2.0 |
| Workload | Deterministic synthetic medallion ETL |
| Input rows | 20,000,000 |
| Custom Spark pool | Memory Optimized, Medium nodes |
| Pool autoscale | 1-10 nodes |
| Dynamic executor allocation | 1-9 executors |
| High Concurrency driver idle timeout | 2 minutes |
| Pipeline dependency graph | Bronze -> Silver -> Gold |
| Repetitions | 2 successful runs per scenario |

The two D measurements are the replacement runs collected after configuring the Live Pool for one hydrated cluster. Earlier D measurements made with three hydrated clusters are excluded from every table and calculation in this document.

## Test matrix

| Scenario | Compute path | High Concurrency behavior | Spark applications per run |
| --- | --- | --- | ---: |
| S | Starter Pool | Distinct session tag per stage | 3 |
| A | On-demand custom pool | Distinct session tag per stage | 3 |
| B | Custom Live Pool | Distinct session tag per stage | 3 |
| C | On-demand custom pool | One shared tag for all stages | 1 |
| D | Custom Live Pool, one hydrated cluster | One shared tag for all stages | 1 |

Workspace-level pipeline High Concurrency was enabled for every scenario. Distinct tags were therefore required for S, A, and B because compatible untagged notebooks can still be grouped on a best-effort basis.

## Data sources and CU attribution

Pipeline and activity timings came from the Fabric REST APIs:

```text
GET  /v1/workspaces/{workspaceId}/items/{pipelineId}/jobs/instances?jobType=Pipeline
POST /v1/workspaces/{workspaceId}/datapipelines/pipelineruns/{runId}/queryactivityruns
```

CU consumption came from `CapacityEvents.CapacityOperations`. This is an Eventhouse table and was queried with KQL. The comparison includes:

- Notebook operations named `Notebook HC Pipeline Run`.
- Environment operations named `Custom Pool Startup` and `Custom Pool Ready`.
- The complete correlated notebook-operation lifecycle, including the two-minute driver tail.

The comparison excludes pipeline `ActivityRun` orchestration CU. Event rows were deduplicated by the CloudEvents `id`.

```text
CU-seconds = capacityUnitMs / 1,000
CU-hours   = capacityUnitMs / 3,600,000
```

For D, each replacement run had its own hydration operation. Environment startup CU is assigned to that run. `Custom Pool Ready` consumption was effectively zero because the single hydrated cluster was acquired almost immediately.

## Raw run results

Times labeled CEST are UTC+02:00 on August 31 and September 1, 2026.

| Scenario | Pipeline run ID | Start UTC | Start CEST | Pipeline s | Bronze s | Silver s | Gold s | Spark applications | Notebook CU-s | Environment startup CU-s | Environment ready CU-s | Included CU-s |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| S1 | `72f8de38-2132-4f2c-be2e-b3189de8a82b` | 2026-08-31 18:08:01 | 2026-08-31 20:08:01 | 679.22 | 247.15 | 233.48 | 183.22 | 3 | 4,653.61 | 0.00 | 0.00 | **4,653.61** |
| S2 | `99567fa7-1233-41a3-a6c9-e75fb86942c3` | 2026-08-31 20:52:08 | 2026-08-31 22:52:08 | 741.66 | 239.30 | 216.04 | 276.36 | 3 | 4,604.42 | 0.00 | 0.00 | **4,604.42** |
| A1 | `c8f514ea-7fb4-4d9a-a971-9da04384251a` | 2026-08-31 18:24:44 | 2026-08-31 20:24:44 | 747.90 | 255.26 | 232.76 | 247.82 | 3 | 2,814.97 | 0.00 | 0.00 | **2,814.97** |
| A2 | `f92cde86-eb99-424e-9578-10f0c67f9fe2` | 2026-08-31 21:04:57 | 2026-08-31 23:04:57 | 723.53 | 232.74 | 232.03 | 246.98 | 3 | 3,144.42 | 0.00 | 0.00 | **3,144.42** |
| B1 | `016540df-17c5-4f44-aa51-ac920de4d94d` | 2026-08-31 20:46:35 | 2026-08-31 22:46:35 | 222.40 | 58.50 | 82.37 | 66.34 | 3 | 6,607.91 | 517.74 | 0.00 | **7,125.65** |
| B2 | `79eb8c83-bb87-4fb0-9820-bdc72f220407` | 2026-08-31 21:18:20 | 2026-08-31 23:18:20 | 209.99 | 51.13 | 81.40 | 66.21 | 3 | 7,820.82 | 524.92 | 0.00 | **8,345.75** |
| C1 | `44f2fe6d-4aa8-450c-b88a-2b2dab264182` | 2026-08-31 18:47:22 | 2026-08-31 20:47:22 | 396.25 | 252.37 | 65.86 | 65.87 | 1 | 1,434.32 | 0.00 | 0.00 | **1,434.32** |
| C2 | `7751a107-bb8c-46b2-a7c4-c9b1b212b1fd` | 2026-08-31 21:23:51 | 2026-08-31 23:23:51 | 385.82 | 271.72 | 66.38 | 35.82 | 1 | 1,426.46 | 0.00 | 0.00 | **1,426.46** |
| D1 | `c1275ac8-a56b-49c8-afa1-16e240f3e6db` | 2026-08-31 22:53:32 | 2026-09-01 00:53:32 | 201.13 | 73.63 | 66.03 | 50.86 | 1 | 2,823.96 | 526.42 | 0.15 | **3,350.53** |
| D2 | `d9cc6556-a007-4851-94e0-181e1f28b71c` | 2026-08-31 23:08:29 | 2026-09-01 01:08:29 | 184.07 | 58.30 | 65.47 | 50.77 | 1 | 3,352.33 | 173.64 | 0.04 | **3,526.01** |

## Live Pool operation details

| Run | Hydration operation ID | Startup CU-s | Ready CU-s | Interpretation |
| --- | --- | ---: | ---: | --- |
| B1 | `f36c5f79-d684-4b50-99fb-f33f9a96234d` | 517.74 | 0.00 | Three independent sessions consumed hydrated capacity |
| B2 | `a676d5a1-eed1-4c23-9fc7-8b4ea68a49a9` | 524.92 | 0.00 | Three independent sessions consumed hydrated capacity |
| D1 | `2b8c55f3-6f1a-47cb-9644-71df8d0db4d1` | 526.42 | 0.15 | One hydrated cluster was acquired almost immediately |
| D2 | `9a233119-5b6e-498e-bd6a-5eb817724b3e` | 173.64 | 0.04 | One hydrated cluster was acquired almost immediately |

The Fabric product team has confirmed that a Live Pool hydrated cluster is single-use for one Spark session within a Live Pool schedule/reactivation cycle. This is a product behavior used for sizing, not a conclusion inferred from these measurements.

## Scenario summary

| Scenario | Average pipeline s | Pipeline range s | Average notebook CU-s | Average environment CU-s | Average included CU-s | Runtime versus S | CU versus S |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| S - Starter, no HC | 710.44 | 679.22-741.66 | 4,629.01 | 0.00 | **4,629.01** | Baseline | Baseline |
| A - On-demand, no HC | 735.71 | 723.53-747.90 | 2,979.70 | 0.00 | **2,979.70** | 3.6% slower | 35.6% lower |
| B - Live Pool, no HC | 216.20 | 209.99-222.40 | 7,214.36 | 521.33 | **7,735.70** | 69.6% faster | 67.1% higher |
| C - On-demand with HC | 391.04 | 385.82-396.25 | 1,430.39 | 0.00 | **1,430.39** | 45.0% faster | 69.1% lower |
| D - Live Pool with HC | 192.60 | 184.07-201.13 | 3,088.14 | 350.12 | **3,438.27** | 72.9% faster | 25.7% lower |

## Derived comparisons

| Comparison | Runtime result | Included-CU result |
| --- | ---: | ---: |
| C versus S | 45.0% faster | 69.1% lower |
| D versus S | 72.9% faster | 25.7% lower |
| D versus B | 10.9% faster | 55.6% lower |
| D versus C | 50.7% faster | 140.4% higher, or about 2.40x |
| A versus S | 3.6% slower | 35.6% lower |

## Analysis

### C delivered the best CU efficiency

High Concurrency without a Live Pool reduced the pipeline from 710.44 seconds to 391.04 seconds while reducing included CU consumption from 4,629.01 to 1,430.39 CU-seconds. Bronze paid the initial application-acquisition cost; Silver and Gold attached to the existing application.

### D delivered the lowest latency

The right-sized combination of one hydrated cluster and one High Concurrency application completed in 192.60 seconds on average. It was 72.9% faster than S and 50.7% faster than C. The latency improvement over C required about 2.40 times C's CU consumption.

### B demonstrated why Live Pool sizing must follow session count

B started quickly, but its three separately tagged notebook stages created three Spark applications. Their lifetimes and two-minute driver tails overlapped, producing the highest average consumption at 7,735.70 CU-seconds. It was 10.9% slower and consumed 2.25 times as many CUs as D.

### A separated lower CU consumption from lower latency

A used fewer CUs than S but had comparable end-to-end duration. Avoiding Starter Pool behavior did not by itself improve pipeline latency when every stage still created an independent on-demand session.

## Interpretation limits

- These are two-run directional measurements, not a statistically complete benchmark.
- The tests used a simplified sequential three-notebook pipeline rather than the larger motivating retail graph.
- S is diagnostic when Starter Pool personalization is affected by managed private endpoints, custom libraries, Spark properties, or other dedicated-provisioning requirements.
- CU-seconds measure capacity efficiency. Under the provisioned F32 model they do not change the fixed capacity invoice, but they affect headroom, smoothing, throttling risk, and opportunity cost.
- Under Autoscale Billing for Spark, the same CU-time differences can affect the bill directly.
- Results should be repeated under representative production concurrency, data layout, and transformation complexity before selecting a production configuration.
