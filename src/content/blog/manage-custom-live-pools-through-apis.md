---
title: 'Deploy, configure, and monitor Custom Live Pools with REST APIs'
description: 'A REST API how-to for deploying, configuring, scheduling, and monitoring Microsoft Fabric Custom Live Pools.'
pubDate: 2026-10-06
tags: ['fabric', 'spark', 'data-engineering', 'rest-api']
draft: false
---

# Deploy, configure, and monitor Custom Live Pools with REST APIs

Custom Live Pools (CLPs) prepare Spark compute before a notebook needs it. The workflow is: select a custom Spark pool, configure an environment, publish it, create an activation schedule, and monitor readiness before running a notebook.

In this article we navigate and end-to-end example on the new APIs following its GA announcement.

## Before you start

Use a workspace on an active paid Fabric capacity, a workspace Admin identity, and a Microsoft Entra access token for `https://api.fabric.microsoft.com`.

All paths below use this base URL:

```text
https://api.fabric.microsoft.com
```

Send these headers with JSON requests:

```http
Authorization: Bearer <accessToken>
Content-Type: application/json
```

## 1. Select a workspace and custom Spark pool

List accessible workspaces and find the exact workspace name:

```http
GET /v1/workspaces
```

Then list its pools:

```http
GET /v1/workspaces/{workspaceId}/spark/pools
```

Follow continuation information when returned. Save the selected custom pool's `id`.

If you need a new pool, create one:

```http
POST /v1/workspaces/{workspaceId}/spark/pools
```

```json
{
  "name": "CLPCompute",
  "nodeFamily": "MemoryOptimized",
  "nodeSize": "Medium",
  "autoScale": {
    "enabled": true,
    "minNodeCount": 1,
    "maxNodeCount": 3
  },
  "dynamicExecutorAllocation": {
    "enabled": true,
    "minExecutors": 1,
    "maxExecutors": 2
  }
}
```

Creation returns `201 Created`. Save the pool ID and read its configuration:

```http
GET /v1/workspaces/{workspaceId}/spark/pools/{poolId}
```

Creating a pool definition is separate from scheduling CLP hydration.

## 2. Create an environment

```http
POST /v1/workspaces/{workspaceId}/environments
```

```json
{
  "displayName": "CLPEnvironment",
  "description": "Scheduled warm Spark compute for notebooks."
}
```

Save the returned environment ID. To find an existing environment instead:

```http
GET /v1/workspaces/{workspaceId}/environments
```

## 3. Configure CLP compute and acceleration

Bind the environment to the selected pool, enable one hydrated cluster, and enable Native Execution Engine (NEE):

```http
PATCH /v1/workspaces/{workspaceId}/environments/{environmentId}/staging/sparkcompute?beta=false
```

```json
{
  "instancePool": {
    "id": "<poolId>"
  },
  "runtimeVersion": "2.0",
  "customLivePoolSupport": "Enabled",
  "customLivePoolSettings": {
    "maxClustersToHydrate": 1,
    "clusterIdleTimeout": "PT20M",
    "customLivePoolLifespan": "PT30M"
  },
  "sparkProperties": [
    {
      "key": "spark.native.enabled",
      "value": "true"
    }
  ]
}
```

Use `beta=false` for this compute contract. `sparkProperties` is an array of key/value objects.

| Setting | Purpose |
| --- | --- |
| `instancePool.id` | Selects the underlying custom Spark pool. |
| `maxClustersToHydrate` | Caps the number of pre-hydrated clusters, not nodes or executors. |
| `clusterIdleTimeout` | Sets the idle timeout; `PT20M` means 20 minutes. |
| `customLivePoolLifespan` | Sets one activation's maximum lifetime; `PT30M` means 30 minutes. |
| `spark.native.enabled` | Enables NEE for the environment. |

These changes are staged. They do not become effective until publication.

Before increasing `maxClustersToHydrate`, read the staging compute for the selected pool:

```http
GET /v1/workspaces/{workspaceId}/environments/{environmentId}/staging/sparkcompute?beta=false
```

The response includes `instancePool.maxClustersToHydrateLimit`, a server-computed upper bound determined by the capacity size associated with the pool. Check that `instancePool.id` matches your selected pool, then choose `maxClustersToHydrate` between 1 and that returned limit. The limit is response metadata, not a setting to send in the PATCH body. The published compute in the next section exposes it too.

## 4. Publish and confirm the configuration

```http
POST /v1/workspaces/{workspaceId}/environments/{environmentId}/staging/publish?beta=false
```

Poll environment metadata:

```http
GET /v1/workspaces/{workspaceId}/environments/{environmentId}
```

Wait for `properties.publishDetails.state` to become `Success`. A `200 OK` publication response with state `Running` is not completion. Correlate the publication's version or timestamps so an earlier `Success` is not mistaken for the current publication.

Read the effective compute:

```http
GET /v1/workspaces/{workspaceId}/environments/{environmentId}/sparkcompute?beta=false
```

A partial example response for this configuration is:

```json
{
  "instancePool": {
    "name": "CLPCompute",
    "type": "Workspace",
    "id": "<poolId>",
    "maxClustersToHydrateLimit": 10
  },
  "runtimeVersion": "2.0",
  "customLivePoolSupport": "Enabled",
  "customLivePoolSettings": {
    "maxClustersToHydrate": 1,
    "clusterIdleTimeout": "PT20M",
    "customLivePoolLifespan": "PT30M"
  },
  "sparkProperties": [
    {
      "key": "spark.native.enabled",
      "value": "true"
    }
  ]
}
```

The limit of 10 is illustrative; use the value returned for your pool.

## 5. Schedule an activation

Use the **environment ID** as the scheduler's item ID and **`RunCustomLivePool`** as the job type:

```http
POST /v1/workspaces/{workspaceId}/items/{environmentId}/jobs/RunCustomLivePool/schedules
```

### Normal interval schedule: Cron

```json
{
  "enabled": true,
  "configuration": {
    "type": "Cron",
    "startDateTime": "2030-01-01T10:00:00Z",
    "endDateTime": "2030-01-01T10:05:00Z",
    "localTimeZoneId": "UTC",
    "interval": 1440
  }
}
```

`interval` is in minutes. This five-minute window is shorter than the 1,440-minute interval, so it admits one occurrence at the start.

### Clock-time schedule: Daily

```json
{
  "enabled": true,
  "configuration": {
    "type": "Daily",
    "startDateTime": "2030-01-01T10:00:00Z",
    "endDateTime": "2030-01-01T10:05:00Z",
    "localTimeZoneId": "UTC",
    "times": ["10:00"]
  }
}
```

Daily uses `times` in `HH:mm` format instead of `interval`. With `localTimeZoneId: "UTC"`, the slot above is 10:00 UTC. When replacing the dates, also replace the slot with a future UTC minute inside the scheduling window.

Both examples are bounded to one eligible occurrence. The window's end does not stop the activation; its lifespan is a separate setting.

Save the returned schedule ID and read it back:

```http
GET /v1/workspaces/{workspaceId}/items/{environmentId}/jobs/RunCustomLivePool/schedules/{scheduleId}
```

Confirm `enabled` and the selected configuration.

> **Keep recurrence and activation lifetime separate.** The idle timeout controls release of an unused hydrated cluster; lifespan limits the activation's duration, while the schedule interval controls when another occurrence is requested. See the final appendix for skipped recurring activations.

## 6. Monitor the activation and wait for readiness

Monitoring uses two views of the same activation, with different job-type names:

| View | Where it comes from | Job type and monitoring fields |
| --- | --- | --- |
| Scheduled occurrence | The schedule created in section 5 triggers a job on the environment item. The Core job scheduler API exposes it under `/items/{environmentId}/jobs/instances`. | `jobType: "RunCustomLivePool"`; `status`, start/end times, and `failureReason`. |
| Spark pool activation | The activation prepares the hydrated clusters. The Environment API exposes its resource-level details under `/environments/{environmentId}/poolActivations`. | `jobType: "SparkPool"`; `state` and the cluster counters in `resourceStatusSummary`. |

First find the scheduled occurrence and save its `id` as `activationId`. Then use that ID to read the Spark pool activation, where it is returned as `jobInstanceId`. These are not two IDs to discover independently: the job-type names describe different API views. The `scheduleId` identifies the recurring schedule definition, not an individual activation.

Find the environment's job instances:

```http
GET /v1/workspaces/{workspaceId}/items/{environmentId}/jobs/instances
```

A partial example response shows an active scheduled occurrence:

```json
{
  "value": [
    {
      "id": "<activationId>",
      "itemId": "<environmentId>",
      "jobType": "RunCustomLivePool",
      "invokeType": "Scheduled",
      "status": "InProgress",
      "startTimeUtc": "2030-01-01T10:00:00Z",
      "endTimeUtc": null,
      "failureReason": null
    }
  ]
}
```

The angle-bracket IDs stand in for GUIDs returned by the service.

Select the scheduled job with `jobType: "RunCustomLivePool"` and the start time corresponding to your activation. In this example, save the entry's `id` as `activationId`; do not use its `itemId` or the schedule ID. Follow continuation information when returned to find the relevant occurrence.

Read its status and any `failureReason`:

```http
GET /v1/workspaces/{workspaceId}/items/{environmentId}/jobs/instances/{activationId}
```

For resource-level aggregate status, list activations or read the specific activation:

```http
GET /v1/workspaces/{workspaceId}/environments/{environmentId}/poolActivations
GET /v1/workspaces/{workspaceId}/environments/{environmentId}/poolActivations/{activationId}
```

The detail response's `jobInstanceId` is the activation job ID. Its `jobType` is `SparkPool`, distinct from the scheduler's `RunCustomLivePool`.

A partial response with one ready cluster looks like this:

```json
{
  "jobInstanceId": "<activationId>",
  "jobType": "SparkPool",
  "state": "InProgress",
  "runtimeVersion": "2.0",
  "maxClustersToHydrate": 1,
  "clusterIdleTimeout": "PT20M",
  "customLivePoolLifespan": "PT30M",
  "resourceStatusSummary": {
    "starting": 0,
    "ready": 1,
    "acquired": 0,
    "stopping": 0,
    "deactivated": 0,
    "expired": 0,
    "cancelled": 0,
    "failed": 0
  }
}
```

For this one-cluster configuration, submit the notebook while the activation is active, `ready == 1`, and `failed == 0`. **Do not wait for the generic job to become `Completed`: ready compute can exist while it is `InProgress`.**

| Signal | How to use it |
| --- | --- |
| `starting` | Hydration is still in progress. |
| `ready` | Warm resources are available. |
| `acquired` | A resource was handed to a consumer; not a live count of running notebooks. |
| `deactivated` / `expired` | Distinguish deactivation from activation-lifetime expiry. |

Activation detail also provides scheduled/actual timestamps, capacity, runtime, hydration settings, and `sparkConfiguration` with reported driver/executor sizing.

`resourceStatusSummary` above contains cluster counters from `poolActivations`; it does not include the Core job's `status`.


## 7. Stop future activations

In order to disable an scheduled CLP, you can do as it follows.
GET the stored schedule first. PATCH requires both `enabled` and its existing `configuration`:

```http
PATCH /v1/workspaces/{workspaceId}/items/{environmentId}/jobs/RunCustomLivePool/schedules/{scheduleId}
```

For the Cron example above, the request body is:

```json
{
  "enabled": false,
  "configuration": {
    "type": "Cron",
    "startDateTime": "2030-01-01T10:00:00Z",
    "endDateTime": "2030-01-01T10:05:00Z",
    "localTimeZoneId": "UTC",
    "interval": 1440
  }
}
```

Use the actual configuration returned by GET, not the illustrative dates. To disable a Daily schedule, retain its `type: "Daily"` and `times` configuration instead of replacing it with Cron.

GET the schedule again and confirm `enabled == false`. Continue monitoring any active activation; disabling the schedule is not an instruction to cancel an already-started run.

## Further reading

- [Update environment Spark compute](https://learn.microsoft.com/en-us/rest/api/fabric/environment/staging/update-spark-compute)
- [Publish an environment](https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/publish-environment)
- [Read published Spark compute](https://learn.microsoft.com/en-us/rest/api/fabric/environment/published/get-spark-compute)
- [Read staging Spark compute](https://learn.microsoft.com/en-us/rest/api/fabric/environment/staging/get-spark-compute)
- [Create an item schedule](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule)
- [List item job instances and job statuses](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances)
- [Manage custom Spark pools](https://learn.microsoft.com/en-us/rest/api/fabric/spark/custom-pools)
- [Native Execution Engine](https://learn.microsoft.com/en-us/fabric/data-engineering/native-execution-engine-overview)

## Appendix: skipped activations (`Deduped`)

A recurring schedule can trigger while another job of the same type is still running. For example, a 30-minute recurrence with a 30-minute activation lifespan can produce a skipped occurrence rather than a fresh activation.

This can also occur with `clusterIdleTimeout: "PT29M"` and `customLivePoolLifespan: "PT30M"` when the schedule's `interval` is 30 minutes. A one-minute difference between idle timeout and lifespan does not guarantee an idle shutdown followed by a successful reactivation one minute later. The activation can reach lifespan expiry instead, and the next scheduled occurrence can be reported as `Deduped` around that transition. **Lifespan is not the reactivation interval:** configure recurrence separately in the schedule and check the job status rather than assuming a fresh activation started.

The Core job scheduler reports this as `status: "Deduped"` in:

```http
GET /v1/workspaces/{workspaceId}/items/{environmentId}/jobs/instances
```

A partial example response shows the existing active occurrence and the later skipped occurrence:

```json
{
  "value": [
    {
      "id": "<activationId>",
      "itemId": "<environmentId>",
      "jobType": "RunCustomLivePool",
      "invokeType": "Scheduled",
      "status": "InProgress",
      "startTimeUtc": "2030-01-01T10:00:00Z",
      "endTimeUtc": null,
      "failureReason": null
    },
    {
      "id": "<skippedJobInstanceId>",
      "itemId": "<environmentId>",
      "jobType": "RunCustomLivePool",
      "invokeType": "Scheduled",
      "status": "Deduped",
      "startTimeUtc": "2030-01-01T10:30:00Z",
      "endTimeUtc": "2030-01-01T10:30:00Z",
      "failureReason": null
    }
  ]
}
```

These timestamps illustrate overlapping occurrences, not the single-occurrence schedules in section 5. The angle-bracket IDs stand in for GUIDs returned by the service.

**`Deduped` is a Core job status, not an error code or a cluster counter.** It means the later occurrence was skipped because another job of the same type was already running; it did not start a fresh activation. It therefore appears in the job-list response, not in `resourceStatusSummary` from `poolActivations`.

Check the existing active occurrence's `activationId` for readiness as described in section 6. Do not substitute `<skippedJobInstanceId>`: requesting activation detail with that skipped ID can return `404 SparkCoreJobNotFound` because the occurrence has no activation detail.
