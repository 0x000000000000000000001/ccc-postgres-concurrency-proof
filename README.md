# PostgreSQL Concurrency Proof

Article: [Event Sourcing Under Concurrent Writes](https://blog.ricofritzsche.de/event-sourcing-under-concurrent-writes-89396e373b71?sk=deef16bcee428953a52f6fb2b3b2e500)

This repository is an executable PostgreSQL proof for two claims from “Event Sourcing Under Concurrent Writes.” It does not assume the article is right: the integration tests either prove or falsify the relevant behavior against a real PostgreSQL server.

The domain example is deliberately tiny: one `UserRegistered` event with a JSON payload such as `{"username":"alice"}`. The command context is all `UserRegistered` events whose payload contains that username, and the context version is `COALESCE(MAX(sequence_number), 0)`.

## What Is Being Tested

The `atomic_cte` schema uses one atomic SQL statement that checks the current context version and inserts only when it matches the expected version. Sequentially, this works: once a conflicting event is already committed, the next statement sees it and inserts nothing.

The concurrent test shows a narrower but important point: under `READ COMMITTED`, statement atomicity is not the same as serializing overlapping writers. Two transactions can both evaluate the context before either inserted row commits, and both can then insert with the same expected context version.

The `serialized` schema establishes a global write order by locking one metadata row with `SELECT ... FOR UPDATE`. While holding that row lock, the implementation recalculates the context version and only then inserts. Every serialized writer must acquire the same metadata-row lock.

## Atomicity Versus Serialization

An atomic CTE makes the check and insert indivisible inside one statement. It does not, by itself, make overlapping statements observe each other’s uncommitted work under `READ COMMITTED`.

The serialized implementation forces append attempts through a database-level order. The row lock is not an application mutex, worker queue, single connection, advisory lock, table lock, or `SERIALIZABLE` transaction. It is a normal PostgreSQL row lock on the metadata row used to allocate the next global sequence number.

## Test Instrumentation

The concurrent `atomic_cte` test installs a `BEFORE INSERT` trigger that waits on a fixed advisory lock. A coordinator connection holds the corresponding session-level advisory lock while two independent append statements start.

This trigger is only test instrumentation. It guarantees that both CTE statements evaluate the context before either insert can commit, removing timing guesses from the test. The advisory lock does not participate in the production context check and is not the proposed concurrency-control solution.

## Why Independent Connections Matter

The tests use independent PostgreSQL connections with distinct `application_name` values. A single connection cannot execute two overlapping transactions, and an application-side mutex or queue would hide the database behavior being tested. Rust only orchestrates concurrency; PostgreSQL provides the consistency behavior.

## Expected Tests

| Test | Expected result |
| --- | --- |
| `atomic_cte_rejects_a_stale_version_when_execution_is_sequential` | The first append succeeds, the stale sequential append inserts no row, and one `alice` event exists. |
| `atomic_cte_allows_two_overlapping_appends_under_read_committed` | Both overlapping CTE statements insert, and two `alice` events exist. |
| `serialized_append_accepts_one_and_rejects_one_for_the_same_context` | One same-context append succeeds, one returns a typed context conflict, and one `alice` event exists. |
| `serialized_append_accepts_both_for_independent_contexts` | Independent `alice` and `bob` appends both succeed with distinct global sequence numbers. |

## Relation To The Article SQL

The serialized implementation corresponds to the article’s simplified SQL by making the global ordering row explicit:

1. Start a `READ COMMITTED` transaction.
2. Lock `serialized.metadata` with `SELECT ... FOR UPDATE`.
3. Recalculate the command context version inside the lock.
4. Roll back and return a typed conflict if the expected version is stale.
5. Otherwise allocate `current_sequence_number + 1`, insert the event, update metadata, and commit.

Run the complete proof with:

```bash
./scripts/verify.sh
```
