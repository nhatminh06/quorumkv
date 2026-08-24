# CLAUDE.md

## Project

QuorumKV is a distributed key-value database written in Go to study consensus, replication, failure, and recovery.

Raft is implemented in this repository from first principles.

Do not use an external Raft implementation.

Long-term architecture:

```text
Client
  ↓
Leader
  ↓
Replicated Raft Log
  ↓
Committed Commands
  ↓
KV State Machine
```

## Development order

Follow this progression:

```text
KV state machine
      ↓
persistent WAL
      ↓
node transport
      ↓
leader election
      ↓
log replication
      ↓
client API
      ↓
crash recovery
      ↓
fault injection
      ↓
snapshots
      ↓
linearizability testing
```

Do not skip ahead to make the repository appear more complete.

## Core priorities

Optimize for:

1. correctness
2. explicit distributed-system invariants
3. deterministic tests
4. persistence correctness
5. observable state transitions
6. failure reproducibility
7. narrow understandable components

Prefer obvious behavior over clever abstractions.

## Git policy

Engineering work must normally be performed on milestone feature branches.

Use:

```text
main
  ↓
milestone/<number>-<name>
  ↓
verified commits
  ↓
push
  ↓
PR
  ↓
review
  ↓
merge
  ↓
synchronize main
```

Commits, pushes, PR creation, and merging are authorized when the active
task requires them.

Never force push or rewrite published history.

Never add AI attribution or AI co-author trailers.

Do not:

* rebase published branch history without approval
* stage unrelated files
* perform destructive Git operations without explicit permission

Before a requested commit report:

* files changed
* diff summary
* tests
* race detector
* proposed commit message

## No AI attribution

Never add references to:

* Claude
* Anthropic
* ChatGPT
* OpenAI
* Copilot
* AI-generated
* generated-by
* assisted-by

Never add AI systems as authors or contributors.

Never add AI `Co-Authored-By` trailers.

## Avoid AI slop

Do not add unnecessary:

* managers
* factories
* interfaces
* wrappers
* frameworks
* logging layers
* configuration layers
* comments
* TODOs
* infrastructure

Prefer concrete concepts:

```text
Node
Term
LogIndex
LogEntry
RequestVote
AppendEntries
StateMachine
WAL
Peer
```

only when the active milestone actually requires them.

Do not create future Raft packages before Raft work begins.

## Language

Use Go.

Prefer the standard library.

Do not introduce large dependencies without a concrete requirement.

In particular, do not use:

* etcd/raft
* Hashicorp Raft
* Dragonboat

or another consensus implementation.

## Determinism

Distributed tests should be deterministic whenever practical.

Do not make correctness tests depend primarily on real wall-clock sleeps.

Where timing matters, prefer injectable clocks/timers or deterministic control once the architecture requires them.

Do not introduce a clock abstraction before timing-sensitive Raft work begins.

## Persistent state

Persistent formats must be explicit.

Do not use language-specific serialization such as `gob` for correctness-critical persistent formats unless explicitly approved.

Validate:

* lengths
* checksums
* record types
* bounds

before allocating or applying data.

Do not silently skip corrupted persistent records.

A torn final record and mid-log corruption are different cases and should be handled deliberately.

## State machine

The KV state machine must be deterministic.

Given the same ordered commands, every node must reach the same state.

Do not introduce nondeterministic information such as:

* current time
* random values
* local environment state

inside replicated command application.

## Ownership

Be explicit about `[]byte` ownership.

Do not let caller mutation silently change stored KV state.

Do not expose mutable internal state unintentionally through reads.

## Raft rules

When Raft implementation begins, important persistent state includes:

```text
currentTerm
votedFor
log
```

Important volatile state eventually includes:

```text
commitIndex
lastApplied
```

Leader-only replication state eventually includes:

```text
nextIndex
matchIndex
```

Do not introduce these until the milestone needs them.

## Persistence before response

When behavior requires persistence before an externally visible action, preserve that ordering.

For example, term/vote state must eventually be persisted before sending responses whose correctness relies on that state.

Do not reorder persistence merely for speed.

## Transport separation

Networking should transport messages.

Raft state-transition logic should decide consensus behavior.

Do not embed election or replication logic directly inside TCP connection handling.

This separation is important for deterministic pure tests.

## Concurrency

Distributed code will become concurrent.

Do not add mutexes preemptively.

When concurrency exists:

* define ownership
* avoid lock-order ambiguity
* keep critical sections small
* do not hold locks during unnecessary blocking I/O
* run the race detector

Never fix a race solely by adding a broad global mutex without understanding the invariant.

## Timeouts

Do not tune timeouts randomly to make flaky tests pass.

When election behavior fails:

1. inspect terms
2. inspect node roles
3. inspect timer resets
4. inspect vote rules
5. identify invariant
6. add regression test
7. fix smallest cause

## Terms

A node receiving a message with a higher term must eventually update its term and step down according to Raft rules once Raft exists.

Do not spread term-transition logic inconsistently across handlers.

Prefer one clearly reasoned mechanism.

## Log correctness

Do not confuse:

```text
appended locally
replicated
committed
applied
```

These are distinct states.

Documentation and code should use these terms accurately.

## Commit semantics

Do not expose an operation as committed simply because the leader wrote it locally.

Once log replication exists, commitment requires Raft's majority/term rules.

Do not invent simplified commitment rules without explicitly limiting the implementation.

## Failure testing

Failure is part of the project, not an afterthought.

Eventually test:

* leader loss
* follower restart
* partitions
* dropped messages
* delayed messages
* stale logs
* divergent uncommitted logs
* recovery

Each failure test should state the invariant being checked.

## Linearizability

Never describe QuorumKV as linearizable merely because Raft is implemented.

Linearizability is an externally observable property.

Prove it with histories/checking once the client layer exists.

## Networking

When transport is introduced:

* bound message sizes
* validate frame lengths
* handle malformed peer input
* avoid unbounded allocations
* distinguish connection errors from consensus state

Do not build a generic RPC framework.

## Tests

Important test categories will include:

```text
state transition
term changes
vote rules
log matching
commit advancement
restart recovery
message loss
partition
stale node
conflicting logs
snapshot catch-up
```

Prefer pure tests of Raft logic before full socket integration.

## Race detector

Run regularly:

```bash
go test -race ./...
```

Race-detector findings must be understood and fixed before piling more concurrency on top.

## Formatting

Run:

```bash
gofmt
```

on modified Go source.

Do not reformat unrelated files for cosmetic reasons.

## Verification

Normal verification should include:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Report exact commands actually run.

Do not claim tests passed unless they were run.

## Benchmarks

Do not invent performance results.

Do not prioritize throughput before consensus correctness.

Later benchmarks must define:

* node count
* replication factor
* command size
* client concurrency
* persistence mode
* hardware
* failure conditions

## Documentation

README should state:

* what QuorumKV is
* what currently works
* what is intentionally not implemented
* how to reproduce tests
* eventually, how failure experiments were performed

Do not turn README into a distributed-systems textbook.

Deep protocol notes can live in `docs/` after the implementation exists.

## Claims

Do not claim:

* fault tolerant
* linearizable
* durable
* strongly consistent
* production-ready
* highly available

without clearly defined and tested evidence.

Prefer:

* implemented
* tested
* observed
* verified
* unsupported

## Scope discipline

If an unrelated defect appears:

* explain it
* do not silently refactor unrelated components
* fix it only if it blocks the active task or is explicitly approved

Keep diffs reviewable.

## Before implementation

For non-trivial work briefly state:

1. what exists
2. what will change
3. the important invariant

Then implement.

Do not produce a long speculative plan.

## After implementation

Report:

### Changed

Meaningful components.

### Invariants

What correctness property is now enforced?

### Verification

Exact commands and outcomes.

### Remaining

Concrete unsupported behavior.

## Definition of done

Code existing is not enough.

A distributed-systems feature should normally have:

* explicit invariant
* deterministic tests
* failure tests
* persistence tests where relevant
* race-detector validation
* accurate documentation
* narrow diff
* no unsupported claims

## Project mindset

When choosing between:

```text
more distributed features
```

and:

```text
proving one consensus invariant
```

prove the invariant.

When choosing between:

```text
real-time integration tests only
```

and:

```text
deterministic state-machine tests
```

prefer deterministic tests first.

When choosing between:

```text
clever concurrency
```

and:

```text
obvious ownership
```

prefer obvious ownership.

When choosing between:

```text
passing flaky tests by increasing timeouts
```

and:

```text
understanding the state transition
```

understand the transition.
