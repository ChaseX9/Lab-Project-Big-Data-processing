# Lab 6, Part 3: Job and CronJob

This lab was done by a group of three students: Thibault Rabbé, Mael Vaudin and Hugo Riviere, ING5 Data IA GR01, ECE Paris. Professor: Joe Azerof.



## Files

- job-simple.yaml | Job | data-import-job
- job-with-retry.yaml | Job | data-transform-job (deadline 300 s)
- job-with-retry-deadline10.yaml | Job | data-transform-job (deadline 10 s)
- job-parallel.yaml | Job | batch-export-job
- cronjob-pipeline.yaml | CronJob | bronze-to-silver-pipeline
- cronjob-backup.yaml | CronJob | daily-backup
- cronjob-etl.yaml | CronJob | etl-bronze-to-silver

## job-simple.yaml

This creates the Job `data-import-job`, which runs one Python container that simulates a data import. It uses `restartPolicy: Never`. The Job completed and the Pod printed its 3 lines:

```
Starting data import...
Processing 1000 records...
Import completed!
```

## job-with-retry.yaml

This creates the Job `data-transform-job`. The container always exits with code 1 to simulate a validation error. The key fields are `backoffLimit: 3` and `activeDeadlineSeconds: 300`. Kubernetes retried the Pod until the backoff limit was reached, and the Job ended as Failed with 4 failed Pods. Each Pod log showed:

```
Transforming records...
ERROR: Validation failed!
```

## job-with-retry-deadline10.yaml

Same Job as above, but with `activeDeadlineSeconds: 10`. We deleted the previous Job and applied this one to see what happens when the deadline is shorter than the retries. This time the Job failed because of the deadline and not because of the backoff limit (see the answers below).

## job-parallel.yaml

This creates the Job `batch-export-job` with `parallelism: 4`, `completions: 4` and `completionMode: Indexed`. The 4 Pods ran at the same time. Each Pod got a different `JOB_COMPLETION_INDEX` (0 to 3) and used it to process its own shard. The whole Job took about 10 s.

```
Pod batch-export-job-0: Exporting shard 0 (users with id % 4 == 0)...
Pod batch-export-job-3: Exporting shard 3 (users with id % 4 == 3)...
batch-export-job   Complete   4/4   10s
```

## cronjob-pipeline.yaml

This creates the CronJob `bronze-to-silver-pipeline` with the schedule `*/2 * * * *`, so it runs every 2 minutes. It also sets `concurrencyPolicy: Forbid`, `successfulJobsHistoryLimit: 3` and `failedJobsHistoryLimit: 1`. Each Job completed in 5 s.

We suspended it with a patch on `spec.suspend`, checked that SUSPEND was True, then resumed it with the same patch set to false:

```
kubectl patch cronjob bronze-to-silver-pipeline -n lab-job-cronjob -p '{"spec":{"suspend":true}}'
```

The container logs showed UTC time (09:54 while it was 11:54 in Paris), because no `timeZone` is set in this CronJob:

```
[2026-10-08 09:54:00.720742] Bronze->Silver ETL started...
[2026-10-08 09:54:02.720882] Completed! 500 records processed
```

## cronjob-backup.yaml

This creates the CronJob `daily-backup`, scheduled at `0 2 * * *` with `timeZone: Europe/Paris`. So it runs every day at 2:00 Paris time and not 2:00 UTC. `kubectl get cronjobs` showed `Europe/Paris` in the TIMEZONE column, while the pipeline CronJob showed `<none>`.

## cronjob-etl.yaml

This creates the CronJob `etl-bronze-to-silver` (every 5 minutes, `concurrencyPolicy: Forbid`, `backoffLimit: 2`, `ttlSecondsAfterFinished: 3600`, with CPU and memory requests and limits). The container reads its settings from the ConfigMap `etl-config` with `envFrom`.

The ConfigMap was not created from a YAML file but with this command:

```
kubectl create configmap etl-config -n lab-job-cronjob --from-literal=BATCH_SIZE=1000 --from-literal=SOURCE_LAYER=bronze --from-literal=TARGET_LAYER=silver
```

To test it without waiting for the schedule, we started a manual run from the CronJob:

```
kubectl create job etl-manual-run --from=cronjob/etl-bronze-to-silver -n lab-job-cronjob
```

The logs showed the values from the ConfigMap:

```
[2026-10-08T09:58:46.392969] ETL: bronze -> silver
[2026-10-08T09:58:46.392969] Batch size: 1000
[2026-10-08T09:58:46.392969] Completed!
```

## Answers to the questions

**1. How many Pods were created and why are they kept?**

4 Pods were created, because `backoffLimit` is 3: 1 initial attempt plus 3 retries. The failed Pods are kept so we can read their logs and find out why the Job failed. The retries were not immediate, they were spaced by an exponential back off (about 10 s, then 20 s, then 40 s).

**2. What is the reason of the Job failure in describe (activeDeadlineSeconds: 300)?**

The reason was `BackoffLimitExceeded` with the message "Job has reached the specified backoff limit". The describe output showed 0 Active, 0 Succeeded and 4 Failed.

**3. What happens with activeDeadlineSeconds: 10?**

The reason became `DeadlineExceeded` with the message "Job was active longer than specified deadline". Only 1 Pod had been created, because the back off delay before the next Pod was longer than the 10 s deadline, so the Job was stopped before any retry.
