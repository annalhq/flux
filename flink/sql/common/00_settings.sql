SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = '${JOB_NAME}';
SET 'parallelism.default' = '${PARALLELISM}';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '${CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout' = '10 s';
