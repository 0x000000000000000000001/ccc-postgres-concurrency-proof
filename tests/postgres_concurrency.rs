use anyhow::{anyhow, Context, Result};
use ccc_postgres_concurrency_proof::{
    connect_pool, naive_atomic_cte_append, naive_atomic_cte_context_version, naive_atomic_cte_matching_event_count, install_naive_atomic_cte_test_gate, acquire_session_advisory_lock, release_session_advisory_lock, lock_free_append, lock_free_context_version,
    lock_free_matching_event_count, reset_database, serialized_append, serialized_context_version,
    serialized_matching_event_count, serialized_metadata_sequence_number, server_version,
    transaction_isolation, AppendOutcome, DEFAULT_DATABASE_URL, OptimisticBatcher,
};
use futures::future::try_join;
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::{Barrier, Mutex};
use tokio::time::{sleep, timeout};

const ALICE: &str = "alice";
const BOB: &str = "bob";

static TEST_DATABASE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn connect_pool_wrapper(url: &str, name: &str) -> sqlx::Result<PgPool> {
    connect_pool(url, name, 10).await
}

async fn test_pool(test_name: &str) -> Result<PgPool> {
    let pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, test_name).await?;
    reset_database(&pool).await?;
    Ok(pool)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serialized_append_accepts_one_and_rejects_one_for_the_same_context() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let coordinator = test_pool("serialized_same_context_coordinator").await?;
    let first_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "serialized_same_context_first").await?;
    let second_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "serialized_same_context_second").await?;

    let (first_observed, second_observed) = try_join(
        serialized_context_version(&first_pool, ALICE),
        serialized_context_version(&second_pool, ALICE),
    )
    .await?;
    assert_eq!(first_observed, 0);
    assert_eq!(second_observed, 0);

    let barrier = Arc::new(Barrier::new(3));
    let first = spawn_serialized_append(first_pool, ALICE, first_observed, Arc::clone(&barrier));
    let second = spawn_serialized_append(second_pool, ALICE, second_observed, Arc::clone(&barrier));

    barrier.wait().await;

    let (first_outcome, second_outcome) = timeout(Duration::from_secs(10), async {
        let first_outcome = first.await.context("first serialized task panicked")??;
        let second_outcome = second.await.context("second serialized task panicked")??;
        Result::<_>::Ok((first_outcome, second_outcome))
    })
    .await
    .context("timed out waiting for same-context serialized appends")??;

    let outcomes = [first_outcome, second_outcome];
    let successes: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            AppendOutcome::Success(success) => Some(success.sequence_number),
            _ => None,
        })
        .collect();
    println!("outcomes: {:?}", outcomes);
    let conflicts: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(conflict) => Some(conflict),
            _ => None,
        })
        .collect();

    assert_eq!(successes.len(), 1);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        serialized_matching_event_count(&coordinator, ALICE).await?,
        1
    );
    assert_eq!(conflicts[0].expected, 0);
    assert_eq!(conflicts[0].actual, successes[0]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serialized_append_accepts_both_for_independent_contexts() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let coordinator = test_pool("serialized_independent_contexts_coordinator").await?;
    let alice_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "serialized_independent_alice").await?;
    let bob_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "serialized_independent_bob").await?;

    let (alice_observed, bob_observed) = try_join(
        serialized_context_version(&alice_pool, ALICE),
        serialized_context_version(&bob_pool, BOB),
    )
    .await?;
    assert_eq!(alice_observed, 0);
    assert_eq!(bob_observed, 0);

    let barrier = Arc::new(Barrier::new(3));
    let alice = spawn_serialized_append(alice_pool, ALICE, alice_observed, Arc::clone(&barrier));
    let bob = spawn_serialized_append(bob_pool, BOB, bob_observed, Arc::clone(&barrier));

    barrier.wait().await;

    let (alice_outcome, bob_outcome) = timeout(Duration::from_secs(10), async {
        let alice_outcome = alice.await.context("alice serialized task panicked")??;
        let bob_outcome = bob.await.context("bob serialized task panicked")??;
        Result::<_>::Ok((alice_outcome, bob_outcome))
    })
    .await
    .context("timed out waiting for independent-context serialized appends")??;

    let alice_sequence_number = match alice_outcome {
        AppendOutcome::Success(success) => success.sequence_number,
        AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(conflict) => {
            return Err(anyhow!("alice unexpectedly conflicted: {conflict:?}"));
        }
        other => return Err(anyhow!("alice unexpectedly failed: {other:?}")),
    };
    let bob_sequence_number = match bob_outcome {
        AppendOutcome::Success(success) => success.sequence_number,
        AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(conflict) => {
            return Err(anyhow!("bob unexpectedly conflicted: {conflict:?}"));
        }
        other => return Err(anyhow!("bob unexpectedly failed: {other:?}")),
    };

    assert_eq!(
        serialized_matching_event_count(&coordinator, ALICE).await?,
        1
    );
    assert_eq!(serialized_matching_event_count(&coordinator, BOB).await?, 1);
    assert_ne!(alice_sequence_number, bob_sequence_number);
    assert_eq!(
        [alice_sequence_number, bob_sequence_number]
            .into_iter()
            .max()
            .expect("two sequence numbers"),
        serialized_metadata_sequence_number(&coordinator).await?
    );
    assert_eq!(serialized_metadata_sequence_number(&coordinator).await?, 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cco_accepts_one_and_rejects_one_for_the_same_context() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let coordinator = test_pool("cco_same_context_coordinator").await?;
    let first_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "cco_same_context_first").await?;
    let second_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "cco_same_context_second").await?;

    let (first_observed, second_observed) = try_join(
        lock_free_context_version(&first_pool, ALICE),
        lock_free_context_version(&second_pool, ALICE),
    )
    .await?;
    assert_eq!(first_observed, 0);
    assert_eq!(second_observed, 0);

    let barrier = Arc::new(Barrier::new(3));
    let first = spawn_cco_append(first_pool.clone(), first_pool, ALICE, first_observed, Arc::clone(&barrier));
    let second = spawn_cco_append(second_pool.clone(), second_pool, ALICE, second_observed, Arc::clone(&barrier));

    barrier.wait().await;

    let (first_outcome, second_outcome) = timeout(Duration::from_secs(10), async {
        let first_outcome = first.await.context("first cco task panicked")??;
        let second_outcome = second.await.context("second cco task panicked")??;
        Result::<_>::Ok((first_outcome, second_outcome))
    })
    .await
    .context("timed out waiting for same-context cco appends")??;

    let outcomes = [first_outcome, second_outcome];
    let successes: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            AppendOutcome::Success(success) => Some(success.sequence_number),
            _ => None,
        })
        .collect();
    println!("outcomes: {:?}", outcomes);
    let conflicts: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            AppendOutcome::ContextCollisionOnOptimisticCheck => Some(0),
            AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(_) => Some(0),
            _ => None,
        })
        .collect();

    assert_eq!(successes.len(), 1);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(
        lock_free_matching_event_count(&coordinator, ALICE).await?,
        1
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cco_accepts_both_for_independent_contexts() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let coordinator = test_pool("cco_independent_contexts_coordinator").await?;
    let alice_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "cco_independent_alice").await?;
    let bob_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "cco_independent_bob").await?;

    let (alice_observed, bob_observed) = try_join(
        lock_free_context_version(&alice_pool, ALICE),
        lock_free_context_version(&bob_pool, BOB),
    )
    .await?;
    assert_eq!(alice_observed, 0);
    assert_eq!(bob_observed, 0);

    let barrier = Arc::new(Barrier::new(3));
    let alice = spawn_cco_append(alice_pool.clone(), alice_pool, ALICE, alice_observed, Arc::clone(&barrier));
    let bob = spawn_cco_append(bob_pool.clone(), bob_pool, BOB, bob_observed, Arc::clone(&barrier));

    barrier.wait().await;

    let (alice_outcome, bob_outcome) = timeout(Duration::from_secs(10), async {
        let alice_outcome = alice.await.context("alice cco task panicked")??;
        let bob_outcome = bob.await.context("bob cco task panicked")??;
        Result::<_>::Ok((alice_outcome, bob_outcome))
    })
    .await
    .context("timed out waiting for independent-context cco appends")??;

    let alice_sequence_number = match alice_outcome {
        AppendOutcome::Success(success) => success.sequence_number,
        other => return Err(anyhow!("alice unexpectedly failed: {:?}", other)),
    };
    let bob_sequence_number = match bob_outcome {
        AppendOutcome::Success(success) => success.sequence_number,
        other => return Err(anyhow!("bob unexpectedly failed: {:?}", other)),
    };

    assert_eq!(
        lock_free_matching_event_count(&coordinator, ALICE).await?,
        1
    );
    assert_eq!(lock_free_matching_event_count(&coordinator, BOB).await?, 1);
    assert_ne!(alice_sequence_number, bob_sequence_number);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_environment_observations() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let pool = test_pool("environment_observations").await?;

    println!("postgres_version={}", server_version(&pool).await?);
    println!(
        "transaction_isolation={}",
        transaction_isolation(&pool).await?
    );

    Ok(())
}

fn spawn_serialized_append(
    pool: PgPool,
    username: &'static str,
    expected_context_version: i64,
    barrier: Arc<Barrier>,
) -> tokio::task::JoinHandle<sqlx::Result<AppendOutcome>> {
    tokio::spawn(async move {
        barrier.wait().await;
        serialized_append(&pool, username, expected_context_version).await
    })
}

fn spawn_cco_append(
    tx_pool: PgPool,
    lock_pool: PgPool,
    username: &'static str,
    expected_context_version: i64,
    barrier: Arc<Barrier>,
) -> tokio::task::JoinHandle<sqlx::Result<AppendOutcome>> {
    tokio::spawn(async move {
        barrier.wait().await;
        lock_free_append(&tx_pool, &lock_pool, username, expected_context_version).await
    })
}


const ATOMIC_GATE_LOCK_KEY: i64 = 74_741_001;

async fn advisory_lock_waiters(pool: &PgPool, advisory_lock_key: i64) -> Result<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT activity.application_name
        FROM pg_stat_activity AS activity
        JOIN pg_locks AS waiting_lock
          ON waiting_lock.pid = activity.pid
        WHERE waiting_lock.locktype = 'advisory'
          AND NOT waiting_lock.granted
          AND waiting_lock.classid = ($1 >> 32)::integer
          AND waiting_lock.objid = ($1 & 4294967295)::integer
          AND activity.wait_event_type = 'Lock'
          AND activity.wait_event = 'advisory'
        "#,
    )
    .bind(advisory_lock_key)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("application_name"))
        .collect())
}


async fn wait_for_advisory_lock_waiters(
    pool: &PgPool,
    application_names: &[&str],
    advisory_lock_key: i64,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);

    while Instant::now() < deadline {
        let waiters = advisory_lock_waiters(pool, advisory_lock_key).await?;
        if application_names
            .iter()
            .all(|application_name| waiters.iter().any(|waiter| waiter == application_name))
        {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }

    Err(anyhow!(
        "timed out waiting for advisory lock waiters: expected {application_names:?}, saw {:?}",
        advisory_lock_waiters(pool, advisory_lock_key).await?
    ))
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn naive_atomic_cte_rejects_a_stale_version_when_execution_is_sequential() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let pool = test_pool("naive_atomic_cte_sequential").await?;

    let observed_context_version = naive_atomic_cte_context_version(&pool, ALICE).await?;
    let first = naive_atomic_cte_append(&pool, ALICE, observed_context_version).await?;
    let second = naive_atomic_cte_append(&pool, ALICE, observed_context_version).await?;

    assert_eq!(observed_context_version, 0);
    assert!(first.is_some());
    assert!(second.is_none());
    assert_eq!(naive_atomic_cte_matching_event_count(&pool, ALICE).await?, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn naive_atomic_cte_allows_two_overlapping_appends_under_read_committed() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let coordinator = test_pool("naive_atomic_cte_coordinator").await?;
    install_naive_atomic_cte_test_gate(&coordinator, ATOMIC_GATE_LOCK_KEY).await?;

    let mut coordinator_connection = PgConnection::connect(DEFAULT_DATABASE_URL).await?;
    acquire_session_advisory_lock(&mut coordinator_connection, ATOMIC_GATE_LOCK_KEY).await?;

    let first_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "naive_atomic_cte_append_first").await?;
    let second_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "naive_atomic_cte_append_second").await?;

    let (first_observed, second_observed) = try_join(
        naive_atomic_cte_context_version(&first_pool, ALICE),
        naive_atomic_cte_context_version(&second_pool, ALICE),
    )
    .await?;

    let first = tokio::spawn(async move { naive_atomic_cte_append(&first_pool, ALICE, 0).await });
    let second = tokio::spawn(async move { naive_atomic_cte_append(&second_pool, ALICE, 0).await });

    wait_for_advisory_lock_waiters(
        &coordinator,
        &["naive_atomic_cte_append_first", "naive_atomic_cte_append_second"],
        ATOMIC_GATE_LOCK_KEY,
    )
    .await?;

    release_session_advisory_lock(&mut coordinator_connection, ATOMIC_GATE_LOCK_KEY).await?;

    let (first_result, second_result) = timeout(Duration::from_secs(10), async {
        let first_result = first.await.context("first append task panicked")??;
        let second_result = second.await.context("second append task panicked")??;
        Result::<_>::Ok((first_result, second_result))
    })
    .await
    .context("timed out waiting for overlapping atomic CTE appends")??;

    assert_eq!(first_observed, 0);
    assert_eq!(second_observed, 0);
    assert!(first_result.is_some());
    assert!(second_result.is_some());
    assert_ne!(
        first_result.expect("first inserted").sequence_number,
        second_result.expect("second inserted").sequence_number
    );
    assert_eq!(
        naive_atomic_cte_matching_event_count(&coordinator, ALICE).await?,
        2
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cco_sieve_rejects_excessive_concurrency() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let coordinator = test_pool("cco_sieve_coordinator").await?;
    
    let sieve_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(50)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET application_name = 'cco_sieve'")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(DEFAULT_DATABASE_URL)
        .await?;

    let observed = lock_free_context_version(&sieve_pool, ALICE).await?;
    assert_eq!(observed, 0);

    let num_tasks = 25; // 25 tasks > 10 MAX_CONTEXT_CONCURRENCY
    let barrier = Arc::new(Barrier::new(num_tasks + 1));
    let mut tasks = Vec::new();

    for _ in 0..num_tasks {
        tasks.push(spawn_cco_append(sieve_pool.clone(), sieve_pool.clone(), ALICE, observed, Arc::clone(&barrier)));
    }

    barrier.wait().await;

    let results = timeout(Duration::from_secs(15), futures::future::try_join_all(
        tasks.into_iter().map(|t| async {
            t.await.context("sieve task panicked")?.map_err(|e| anyhow::anyhow!(e))
        })
    )).await.context("timed out waiting for sieve appends")??;

    let mut successes = 0;
    let mut occ_conflicts = 0;
    let mut sieve_conflicts = 0;

    for result in results {
        match result {
            AppendOutcome::Success(_) => successes += 1,
            AppendOutcome::ContextCollisionOnOptimisticCheck => occ_conflicts += 1,
            AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(_) => occ_conflicts += 1,
            AppendOutcome::ContextCollisionOnIntentRegistration => sieve_conflicts += 1,
            _ => {},
        }
    }

    assert_eq!(successes, 1);
    assert!(sieve_conflicts > 0, "Sieve should have rejected at least one task out of {}, got {} OCC conflicts", num_tasks, occ_conflicts);
    assert_eq!(
        lock_free_matching_event_count(&coordinator, ALICE).await?,
        1
    );

    Ok(())
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cco_rejects_a_stale_version_when_execution_is_sequential() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let pool = test_pool("cco_stale_sequential").await?;

    // On observe la version initiale (0)
    let observed_context_version = lock_free_context_version(&pool, ALICE).await?;
    assert_eq!(observed_context_version, 0);

    // On insère un premier événement avec succès
    let first = lock_free_append(&pool, &pool, ALICE, observed_context_version).await?;
    assert!(matches!(first, AppendOutcome::Success(_)));

    // On essaie d'insérer un second événement en utilisant l'ancienne version observée (0) au lieu de 1
    let second = lock_free_append(&pool, &pool, ALICE, observed_context_version).await?;
    
    // On vérifie que la base a rejeté l'insertion avec la bonne erreur et le bon détail
    match second {
        AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(conflict) => {
            assert_eq!(conflict.expected, 0);
            assert_eq!(conflict.actual, 1);
        },
        other => return Err(anyhow::anyhow!("Expected SequenceNumberInconsistencyOnOptimisticCheck, got {:?}", other)),
    }

    assert_eq!(lock_free_matching_event_count(&pool, ALICE).await?, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batcher_flushes_by_size() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let tx_pool = test_pool("batcher_flushes_by_size_tx").await?;
    let lock_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "batcher_flushes_by_size_lock").await?;
    
    let batcher = OptimisticBatcher::new(tx_pool.clone(), lock_pool.clone());
    
    let mut futures = Vec::new();
    let start = Instant::now();
    for i in 0..100 {
        let b = batcher.clone();
        futures.push(tokio::spawn(async move {
            b.append(format!("user_{}", i), 0).await
        }));
    }
    
    for f in futures {
        let res = f.await??;
        assert!(matches!(res, AppendOutcome::Success(_)));
    }
    let elapsed = start.elapsed();
    
    // Size flush is immediate (doesn't wait 5ms), but give some leeway for execution
    assert!(elapsed < Duration::from_millis(1500));
    
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batcher_flushes_by_time() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let tx_pool = test_pool("batcher_flushes_by_time_tx").await?;
    let lock_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "batcher_flushes_by_time_lock").await?;
    
    let batcher = OptimisticBatcher::new(tx_pool.clone(), lock_pool.clone());
    
    let start = Instant::now();
    // Send only 1 request (well below 100)
    let res = batcher.append("time_user".to_string(), 0).await?;
    let elapsed = start.elapsed();
    
    assert!(matches!(res, AppendOutcome::Success(_)));
    // Should take AT LEAST 5ms
    assert!(elapsed >= Duration::from_millis(4));
    
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batcher_resolves_internal_conflicts() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let tx_pool = test_pool("batcher_internal_conflicts_tx").await?;
    let lock_pool = connect_pool_wrapper(DEFAULT_DATABASE_URL, "batcher_internal_conflicts_lock").await?;
    
    let batcher = OptimisticBatcher::new(tx_pool.clone(), lock_pool.clone());
    
    let mut futures = Vec::new();
    // 3 requests to the same user in the same batch
    for _ in 0..3 {
        let b = batcher.clone();
        futures.push(tokio::spawn(async move {
            b.append("conflict_user".to_string(), 0).await
        }));
    }
    
    // Fill the rest to force size flush quickly
    for i in 3..100 {
        let b = batcher.clone();
        futures.push(tokio::spawn(async move {
            b.append(format!("filler_{}", i), 0).await
        }));
    }
    
    let mut successes = 0;
    let mut conflicts = 0;
    
    for _ in 0..3 {
        let f = futures.remove(0);
        let res = f.await.unwrap()?;
        match res {
            AppendOutcome::Success(_) => successes += 1,
            AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(_) => conflicts += 1,
            AppendOutcome::ContextCollisionOnOptimisticCheck => conflicts += 1,
            AppendOutcome::ContextCollisionOnIntentRegistration => conflicts += 1,
            _ => panic!("Unexpected outcome: {:?}", res),
        }
    }
    
    assert_eq!(successes, 1);
    assert_eq!(conflicts, 2);
    
    Ok(())
}
