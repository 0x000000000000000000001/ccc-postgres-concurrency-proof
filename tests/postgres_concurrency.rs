use anyhow::{anyhow, Context, Result};
use ccc_postgres_concurrency_proof::{
    acquire_session_advisory_lock, atomic_cte_append, atomic_cte_context_version,
    atomic_cte_matching_event_count, connect_pool, install_atomic_cte_test_gate,
    release_session_advisory_lock, reset_database, serialized_append, serialized_context_version,
    serialized_matching_event_count, serialized_metadata_sequence_number, server_version,
    transaction_isolation, AppendOutcome, DEFAULT_DATABASE_URL,
};
use futures::future::try_join;
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::{Barrier, Mutex};
use tokio::time::{sleep, timeout};

const ALICE: &str = "alice";
const BOB: &str = "bob";
const ATOMIC_GATE_LOCK_KEY: i64 = 74_741_001;

static TEST_DATABASE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

async fn test_pool(test_name: &str) -> Result<PgPool> {
    let pool = connect_pool(DEFAULT_DATABASE_URL, test_name).await?;
    reset_database(&pool).await?;
    Ok(pool)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_cte_rejects_a_stale_version_when_execution_is_sequential() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let pool = test_pool("atomic_cte_sequential").await?;

    let observed_context_version = atomic_cte_context_version(&pool, ALICE).await?;
    let first = atomic_cte_append(&pool, ALICE, observed_context_version).await?;
    let second = atomic_cte_append(&pool, ALICE, observed_context_version).await?;

    assert_eq!(observed_context_version, 0);
    assert!(first.is_some());
    assert!(second.is_none());
    assert_eq!(atomic_cte_matching_event_count(&pool, ALICE).await?, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_cte_allows_two_overlapping_appends_under_read_committed() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let coordinator = test_pool("atomic_cte_coordinator").await?;
    install_atomic_cte_test_gate(&coordinator, ATOMIC_GATE_LOCK_KEY).await?;

    let mut coordinator_connection = PgConnection::connect(DEFAULT_DATABASE_URL).await?;
    acquire_session_advisory_lock(&mut coordinator_connection, ATOMIC_GATE_LOCK_KEY).await?;

    let first_pool = connect_pool(DEFAULT_DATABASE_URL, "atomic_cte_append_first").await?;
    let second_pool = connect_pool(DEFAULT_DATABASE_URL, "atomic_cte_append_second").await?;

    let (first_observed, second_observed) = try_join(
        atomic_cte_context_version(&first_pool, ALICE),
        atomic_cte_context_version(&second_pool, ALICE),
    )
    .await?;

    let first = tokio::spawn(async move { atomic_cte_append(&first_pool, ALICE, 0).await });
    let second = tokio::spawn(async move { atomic_cte_append(&second_pool, ALICE, 0).await });

    wait_for_advisory_lock_waiters(
        &coordinator,
        &["atomic_cte_append_first", "atomic_cte_append_second"],
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
        atomic_cte_matching_event_count(&coordinator, ALICE).await?,
        2
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serialized_append_accepts_one_and_rejects_one_for_the_same_context() -> Result<()> {
    let _guard = TEST_DATABASE_LOCK.lock().await;
    let coordinator = test_pool("serialized_same_context_coordinator").await?;
    let first_pool = connect_pool(DEFAULT_DATABASE_URL, "serialized_same_context_first").await?;
    let second_pool = connect_pool(DEFAULT_DATABASE_URL, "serialized_same_context_second").await?;

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
            AppendOutcome::ContextConflict(_) => None,
        })
        .collect();
    let conflicts: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            AppendOutcome::Success(_) => None,
            AppendOutcome::ContextConflict(conflict) => Some(conflict),
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
    let alice_pool = connect_pool(DEFAULT_DATABASE_URL, "serialized_independent_alice").await?;
    let bob_pool = connect_pool(DEFAULT_DATABASE_URL, "serialized_independent_bob").await?;

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
        AppendOutcome::ContextConflict(conflict) => {
            return Err(anyhow!("alice unexpectedly conflicted: {conflict:?}"));
        }
    };
    let bob_sequence_number = match bob_outcome {
        AppendOutcome::Success(success) => success.sequence_number,
        AppendOutcome::ContextConflict(conflict) => {
            return Err(anyhow!("bob unexpectedly conflicted: {conflict:?}"));
        }
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
