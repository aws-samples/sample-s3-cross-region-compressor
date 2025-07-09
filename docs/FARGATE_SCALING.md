# Fargate Scaling Strategy

This document describes the autoscaling strategy implemented for ECS Fargate services in the S3 Cross-Region Compressor system.

## Overview

The S3 Cross-Region Compressor uses a autoscaling approach for its ECS Fargate services that is optimized for SQS-based workloads. The scaling strategy is designed to:

- Scale out quickly when the backlog increases
- Scale in gracefully when the workload decreases
- Scale to zero when idle to minimize costs
- Maintain optimal task-to-message ratio for efficiency

## Scaling Metrics

The scaling system is built around several key CloudWatch metrics and mathematical expressions:

### Primary Metrics

| Metric | Description |
|--------|-------------|
| **SQS Visible Messages** | Number of messages visible in the queue, representing pending work |
| **SQS In-Flight Messages** | Number of messages being processed but not yet deleted |
| **Running Task Count** | Number of active ECS tasks currently running |
| **Desired Task Count** | Number of tasks ECS is trying to maintain |

### Derived Expressions

| Expression | Description | Purpose |
|------------|-------------|---------|
| **Backlog Per Task** | Visible messages ÷ running tasks | Primary scaling decision metric |
| **Queue Empty** | Detects when both visible and in-flight messages are zero | Used for scaling to zero |
| **Low Backlog Multiple Tasks** | Detects when backlog is low but multiple tasks are running | Used for scaling in |

## Scaling Strategies

### Scale Out

When the backlog per task exceeds the target threshold, the system scales out with progressively larger increments:

| Backlog Range | Tasks Added |
|--------------|-------------|
| 0 to target | +1 task |
| target to 2×target | +2 tasks |
| 2×target to 3×target | +3 tasks |
| > 3×target | +4 tasks |

* Target is defined as `scaling_target_backlog_per_task` in the `replication_config.json` file.

This progressive scaling ensures rapid response to larger backlogs while avoiding overprovisioning for smaller spikes.

### Scale to Zero

The system scales to zero tasks when:
- The queue has no visible messages
- There are no in-flight messages
- This condition persists for 3 consecutive evaluation periods

This aggressive scaling-to-zero approach minimizes costs during idle periods but ensures quick scale-up when new messages arrive.

### Scale In

When the backlog per task falls below 50% of the target and multiple tasks are running, the system scales in by 1 task. This gradual scale-in prevents oscillation and maintains capacity for minor workload fluctuations.

## Implementation Details

### CloudWatch Alarms

Three key CloudWatch alarms drive the scaling actions:

1. **High Backlog Per Task Alarm**:
   - Triggers when backlog per task > target
   - Initiates step scaling out

2. **Queue Empty Alarm**:
   - Triggers when no messages exist (visible or in-flight)
   - Initiates scaling to zero tasks

3. **Low Backlog Multiple Tasks Alarm**:
   - Triggers when backlog per task < 50% of target and multiple tasks running
   - Initiates scaling in by 1 task

### Math Expressions

The system uses CloudWatch math expressions to calculate complex conditions:

```
# Backlog per task expression
IF(FILL(running_tasks,0) < 1 AND visible_messages > 0 AND visible_messages < target_backlog, 
   target_backlog + 1, 
   visible_messages/IF(FILL(running_tasks,0) < 1, 1, running_tasks))
```

This handles the special case when there are messages but no tasks running, ensuring scale-up from zero.

```
# Queue empty expression
IF(visible_messages + in_flight_messages == 0, 1, 0)
```

Returns 1 when queue is completely empty, triggering scale-to-zero.

```
# Low backlog multiple tasks expression
IF(visible_messages/IF(FILL(running_tasks,0) < 1, 1, running_tasks) <= target_backlog/2, 
   IF(running_tasks > 1, 1, 0), 
   0)
```

Returns 1 when backlog is low but multiple tasks are running.

## Configuration Parameters

The scaling system can be tuned with these parameters:

| Parameter | Description | Default | Impact |
|-----------|-------------|---------|--------|
| **scaling_target_backlog_per_task** | Target number of messages per task | 60 | Higher values improve cost efficiency but increase processing latency |
| **max_capacity** | Maximum number of tasks | 20 | Upper limit on scaling to prevent runaway costs |
| **scale_out_cooldown** | Seconds between scale-out actions | 60 | Shorter enables faster response, longer prevents thrashing |
| **scale_in_cooldown** | Seconds between scale-in actions | 120 | Longer provides stability during fluctuating workloads |

## Example Scaling Scenarios

### Scenario 1: Sudden Queue Growth

When a large batch of objects is uploaded:

1. Queue size increases rapidly
2. Backlog per task exceeds target
3. Scale-out action adds multiple tasks based on backlog depth
4. Additional tasks process messages in parallel
5. Backlog per task returns to target level

### Scenario 2: Gradual Queue Reduction

As queue size diminishes:

1. Backlog per task decreases below 50% of target
2. Scale-in alarm triggers after 2 evaluation periods
3. One task is removed
4. Process repeats gradually until workload normalizes

### Scenario 3: Queue Emptying

When processing completes:

1. Queue becomes completely empty (no visible or in-flight messages)
2. Empty queue condition persists for 3 evaluation periods
3. Scale-to-zero action sets desired count to zero
4. Tasks terminate after completing in-progress work
5. No costs incurred until new messages arrive

## Advantages of This Approach

1. **Cost Efficiency**: Scales to zero when idle, minimizing costs
2. **Responsiveness**: Scales quickly when backlog increases
3. **Stability**: Avoids oscillation through cooldowns and gradual scale-in
4. **Optimization**: Maintains ideal backlog-to-task ratio
5. **Resilience**: Special handling for scale-from-zero condition

## Integration with CloudWatch

The scaling metrics and alarms are available in CloudWatch under:

- **ECS/ContainerInsights** namespace for task metrics
- **AWS/SQS** namespace for queue metrics

CloudWatch dashboards can be created to visualize:
- Queue depth over time
- Running task count
- Backlog per task ratio
- Scaling action history

## Related Documentation

- For more details on the overall architecture, see [ARCHITECTURE.md](ARCHITECTURE.md)
- For cost optimization strategies, see [COST_OPTIMIZATION.md](COST_OPTIMIZATION.md)
- For monitoring guidance, see [MONITORING.md](MONITORING.md)
