
# Member 3: Distributed Coordination, Failure Detection and Recovery

## Design

The system will use gRPC for communication between distributed components. Worker health will be monitored through heartbeat RPCs and deadlines.

A missed heartbeat will initially mark a worker as suspected failed rather than immediately declaring it dead. The system will retry the health check before removing the worker from normal task allocation.

Coordinator election will use the group's existing election rules. Coordinator terms will be tracked so that messages from an outdated coordinator can be rejected.

Job and task information will be recorded in a recovery ledger. Completed task results will be checkpointed, while unfinished tasks will be made available for reassignment.

## Recovery goals

- Detect unavailable or slow workers.
- Elect a replacement coordinator when required.
- Preserve completed results and unfinished task information.
- Reassign unfinished tasks without counting duplicate results.
- Allow newly joined workers to receive tasks.

## Implementation limitation

A local checkpoint protects against a process restart only while its storage remains available. Protection against loss of the coordinator's machine requires replicated or shared durable state.
