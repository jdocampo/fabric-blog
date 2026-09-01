---
title: 'Why your Fabric notebook takes three minutes to start — and when it does not'
description: 'Starter pools, custom pools and the session-level settings that quietly opt you out of a warm cluster.'
pubDate: 2026-06-24
tags: ['fabric', 'spark']
---

Fabric keeps pre-warmed Spark clusters ready so a notebook attaches in seconds instead
of waiting for nodes to provision. Most cold starts come from doing something that makes
your session ineligible for one.

## What breaks the warm path

A starter pool session only stays warm while your requested configuration matches the
pre-warmed shape. These push you off it:

- A custom pool with a node size the starter pool does not have
- Requesting a Spark version other than the workspace default
- Certain `spark.conf` values that must be set before the JVM starts, notably driver and
  executor memory overrides

The last one catches people out, because the setting looks harmless in a notebook cell:

```python
# forces a fresh cluster — this cannot be applied to a running session
spark.conf.set("spark.executor.memory", "28g")
```

## Checking what you actually got

The session tells you, if you ask:

```python
print(spark.sparkContext.applicationId)
print(spark.conf.get("spark.executor.instances"))
print(spark.sparkContext.getConf().get("spark.master"))
```

Compare a run that started fast with one that did not. The configuration diff is the
answer, and it is usually one line someone added months ago to fix an
out-of-memory error that no longer exists.

## The practical rule

Keep exploratory notebooks on the workspace default so they attach fast. Put the memory
tuning in the scheduled pipeline definitions, where a three-minute start costs nothing
and nobody is sitting watching the spinner.
