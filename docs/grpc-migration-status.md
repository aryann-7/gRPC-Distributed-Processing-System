# Network migration status

The default WorkerMain still uses RMI Bootstrap discovery, neighbours, election,
coordinator publication and task dispatch. It has not switched to gRPC topology.
No second topology map has been added.

## Verified dispatch stage

CoordinatorManager can accept WorkerGrpcClient instances through its existing
local WorkerService interface. They share a retained PeerGrpcClient channel for
worker and election RPC stubs. Task dispatch consumes ExecuteTaskStream, forwards
real work counters and requires its final TaskResultResponse. The existing task
partitioning, aggregation, JAC and five-job term rules are unchanged.

The worker stream now includes its computed final result and assignment identity.
Closing CoordinatorManager stops its dispatch executor. Its existing RMI export
behaviour is unchanged; only task communication through WorkerGrpcClient is gRPC.

This is a tested transport capability, not a completed worker startup migration.
Bootstrap heartbeats, membership consumption and coordinator client RPC handlers
still need to be connected to the runtime.

## Runtime integration still required

One authoritative neighbour collection must hold the typed descriptor together
with its gRPC client. PeerService must register into that collection. Election
handlers must delegate to WorkerNode's existing flood/reply methods. Bootstrap
registration must advertise the actual bound gRPC port and retain the session
and registry epoch. Shutdown must cancel watching, renewal and retained channels.

The combined WorkerNode integration patches were rejected by automatic approval
review because they coupled export mode, constructors, topology and shutdown
without verified integration coverage. Those patches were not applied.

## Teammate contract conflict

Do not merge member3-failure-recovery automatically. Commit
cd4c35a2bf869166fbf55203271647f838787907 adds ElectionRequest and
CoordinatorAnnouncement to worker.proto with the same distrilab/common.grpc names
as election.proto but different fields and wire numbers. For example our
ElectionRequest field 1 is string election_id; theirs is int32 candidate_id.
Their request has no immediate sender ID. Their CoordinatorElectionService uses
StartElection and an ElectionResponse, whereas ours uses ReceiveElection,
ReceiveElectionReply and acknowledgement-only responses. Their announcement also
lacks our forwarding message ID and worker session. Reconcile these contracts
with the election owner before merging; do not replace our election.proto.
