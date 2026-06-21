use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction, PgConnection};
use std::time::Duration;

pub const DEFAULT_DATABASE_URL: &str =
    "postgres://postgres:postgres@localhost:5432/concurrency_proof";

pub const USER_REGISTERED: &str = "UserRegistered";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSuccess {
    pub sequence_number: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextConflict {
    pub expected: i64,
    pub actual: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    Success(AppendSuccess),
    ContextCollisionOnIntentRegistration,
    IntentRegistrationError,
    ContextCollisionOnOptimisticCheck,
    SequenceNumberInconsistencyOnOptimisticCheck(ContextConflict),
}

use tokio::sync::{mpsc, oneshot};
use std::time::Instant;

pub struct BatchRequest {
    pub username: String,
    pub expected_context_version: i64,
    pub responder: oneshot::Sender<sqlx::Result<AppendOutcome>>,
}

#[derive(Clone)]
pub struct OptimisticBatcher {
    sender: mpsc::Sender<BatchRequest>,
}

impl OptimisticBatcher {
    pub fn new(tx_pool: PgPool, lock_pool: PgPool) -> Self {
        let (sender, mut receiver) = mpsc::channel::<BatchRequest>(10000);
        
        tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(100);
            let mut oldest_event: Option<Instant> = None;
            
            loop {
                let wait_duration = if let Some(oldest) = oldest_event {
                    let elapsed = oldest.elapsed();
                    if elapsed >= Duration::from_millis(5) {
                        Duration::from_millis(0)
                    } else {
                        Duration::from_millis(5) - elapsed
                    }
                } else {
                    Duration::from_secs(86400) // Sleep until message arrives
                };

                let recv_result = tokio::time::timeout(wait_duration, receiver.recv()).await;
                
                match recv_result {
                    Ok(Some(req)) => {
                        if buffer.is_empty() {
                            oldest_event = Some(Instant::now());
                        }
                        buffer.push(req);
                        
                        if buffer.len() >= 100 {
                            let batch_to_flush = std::mem::replace(&mut buffer, Vec::with_capacity(100));
                            let tx_p = tx_pool.clone();
                            let lock_p = lock_pool.clone();
                            tokio::spawn(async move {
                                Self::flush(tx_p, lock_p, batch_to_flush).await;
                            });
                            oldest_event = None;
                        }
                    }
                    Ok(None) => break, // Channel closed
                    Err(_) => {
                        // Timeout (5ms reached)
                        if !buffer.is_empty() {
                            let batch_to_flush = std::mem::replace(&mut buffer, Vec::with_capacity(100));
                            let tx_p = tx_pool.clone();
                            let lock_p = lock_pool.clone();
                            tokio::spawn(async move {
                                Self::flush(tx_p, lock_p, batch_to_flush).await;
                            });
                            oldest_event = None;
                        }
                    }
                }
            }
        });
        
        Self { sender }
    }
    
    pub async fn append(&self, username: String, expected_context_version: i64) -> sqlx::Result<AppendOutcome> {
        let (tx, rx) = oneshot::channel();
        let req = BatchRequest {
            username,
            expected_context_version,
            responder: tx,
        };
        
        if self.sender.send(req).await.is_err() {
            return Err(sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Batcher closed")));
        }
        
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Batcher closed unexpectedly"))),
        }
    }

    async fn flush(tx_pool: PgPool, lock_pool: PgPool, mut buffer: Vec<BatchRequest>) {
        if buffer.is_empty() {
            return;
        }

        let drained: Vec<BatchRequest> = buffer.drain(..).collect();
        let mega_res = lock_free_append_mega_batch(&tx_pool, &lock_pool, &drained).await;

        match mega_res {
            Ok(Some(outcomes)) => {
                // Mega-batch succeeded completely!
                for (req, outcome) in drained.into_iter().zip(outcomes.into_iter()) {
                    let _ = req.responder.send(Ok(outcome));
                }
            }
            Ok(None) => {
                // Mega-batch failed (collision or stale state). Fallback to atomized execution.
                let mut handles = Vec::with_capacity(drained.len());
                for req in &drained {
                    let tx_pool = tx_pool.clone();
                    let lock_pool = lock_pool.clone();
                    let username = req.username.clone();
                    let expected_v = req.expected_context_version;
                    handles.push(tokio::spawn(async move {
                        lock_free_append(&tx_pool, &lock_pool, &username, expected_v).await
                    }));
                }
                
                let mut fallback_outcomes = Vec::with_capacity(handles.len());
                for handle in handles {
                    fallback_outcomes.push(match handle.await {
                        Ok(res) => res,
                        Err(e) => Err(sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))),
                    });
                }
                
                for (req, outcome_res) in drained.into_iter().zip(fallback_outcomes.into_iter()) {
                    let _ = req.responder.send(outcome_res);
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                for req in drained.into_iter() {
                    let err = sqlx::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, err_msg.clone()));
                    let _ = req.responder.send(Err(err));
                }
            }
        }
    }
}

use sqlx::postgres::PgConnectOptions;
use std::str::FromStr;

pub async fn connect_pool(database_url: &str, application_name: &str, max_conns: u32) -> sqlx::Result<PgPool> {
    let options = PgConnectOptions::from_str(database_url)?
        .application_name(application_name)
        .statement_cache_capacity(0); // CRITICAL: Disable for PgBouncer transaction mode

    PgPoolOptions::new()
        .max_connections(max_conns)
        .acquire_timeout(Duration::from_secs(120))
        .connect_with(options)
        .await
}

pub async fn reset_database(pool: &PgPool) -> sqlx::Result<()> {
    for migration in [
        include_str!("../sql/001_atomic_cte.sql"),
        include_str!("../sql/002_serialized.sql"),
        include_str!("../sql/004_lock_free.sql"),
    ] {
        for statement in migration.split(';') {
            let statement = statement.trim();
            if !statement.is_empty() {
                sqlx::query(statement).execute(pool).await?;
            }
        }
    }

    Ok(())
}

pub async fn transaction_isolation(pool: &PgPool) -> sqlx::Result<String> {
    sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(pool)
        .await
}

pub async fn server_version(pool: &PgPool) -> sqlx::Result<String> {
    sqlx::query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await
}

pub async fn serialized_context_version(pool: &PgPool, username: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COALESCE(MAX(sequence_number), 0)
        FROM serialized.events
        WHERE event_type = 'UserRegistered'
          AND payload @> jsonb_build_object('username', $1)
        "#,
    )
    .bind(username)
    .fetch_one(pool)
    .await
}

pub async fn serialized_matching_event_count(pool: &PgPool, username: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM serialized.events
        WHERE event_type = 'UserRegistered'
          AND payload @> jsonb_build_object('username', $1)
        "#,
    )
    .bind(username)
    .fetch_one(pool)
    .await
}

pub async fn serialized_metadata_sequence_number(pool: &PgPool) -> sqlx::Result<i64> {
    sqlx::query_scalar("SELECT current_sequence_number FROM serialized.metadata WHERE id = TRUE")
        .fetch_one(pool)
        .await
}

pub async fn serialized_append(
    pool: &PgPool,
    username: &str,
    expected_context_version: i64,
) -> sqlx::Result<AppendOutcome> {
    let mut tx = pool.begin().await?;
    set_read_committed(&mut tx).await?;

    let locked_sequence_number: i64 = sqlx::query_scalar(
        r#"
        SELECT current_sequence_number
        FROM serialized.metadata
        WHERE id = TRUE
        FOR UPDATE
        "#,
    )
    .fetch_one(&mut *tx)
    .await?;

    let actual_context_version = serialized_context_version_in_tx(&mut tx, username).await?;

    if actual_context_version != expected_context_version {
        tx.rollback().await?;
        return Ok(AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(ContextConflict {
            expected: expected_context_version,
            actual: actual_context_version,
        }));
    }

    let next_sequence_number = locked_sequence_number + 1;

    sqlx::query(
        r#"
        INSERT INTO serialized.events (sequence_number, event_type, payload)
        VALUES ($1, 'UserRegistered', jsonb_build_object('username', $2))
        "#,
    )
    .bind(next_sequence_number)
    .bind(username)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE serialized.metadata
        SET current_sequence_number = $1
        WHERE id = TRUE
        "#,
    )
    .bind(next_sequence_number)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(AppendOutcome::Success(AppendSuccess {
        sequence_number: next_sequence_number,
    }))
}

async fn set_read_committed(tx: &mut Transaction<'_, Postgres>) -> sqlx::Result<()> {
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn serialized_context_version_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    username: &str,
) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COALESCE(MAX(sequence_number), 0)
        FROM serialized.events
        WHERE event_type = 'UserRegistered'
          AND payload @> jsonb_build_object('username', $1)
        "#,
    )
    .bind(username)
    .fetch_one(&mut **tx)
    .await
}

pub async fn lock_free_context_version(pool: &PgPool, username: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COALESCE(MAX(sequence_number), 0)
        FROM lock_free.events
        WHERE event_type = 'UserRegistered'
          AND payload @> jsonb_build_object('username', $1)
        "#,
    )
    .bind(username)
    .fetch_one(pool)
    .await
}

pub async fn lock_free_matching_event_count(pool: &PgPool, username: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM lock_free.events
        WHERE event_type = 'UserRegistered'
          AND payload @> jsonb_build_object('username', $1)
        "#,
    )
    .bind(username)
    .fetch_one(pool)
    .await
}

pub async fn lock_free_append(
    tx_pool: &PgPool,
    lock_pool: &PgPool,
    username: &str,
    expected_context_version: i64,
) -> sqlx::Result<AppendOutcome> {
    const MAX_CONTEXT_CONCURRENCY: i64 = 10;

    let json_context = format!("$[*] ? (@.username == \"{}\")", username);
    let event_payload = format!("[{{\"username\": \"{}\"}}]", username);
    let intent_id = uuid::Uuid::new_v4();

    let lock_key: i64 = rand::random::<i64>();

    // --- APP BOUNDARY ---
    // At this point in a real application, the Command Handler (or RPU) has ALREADY done the job.
    // The application has made its `decide()`, generated the `event`(s), and identified the `context`.
    // The Event Store is now blindly executing the final "Append" phase, focusing solely on data integrity.

    // We must acquire a dedicated connection for session-level locks
    let mut lock_conn = lock_pool.acquire().await?;
    
    // Acquire our intent's session lock immediately
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(&mut *lock_conn)
        .await?;

    // 1. Intent registration with Zombie GC & Sieve (Auto-commit)
    //    The Sieve acts as a "Bouncer" to protect the transaction pool under high contention.
    //    If too many concurrent requests target the exact same context (e.g., >= MAX_CONTEXT_CONCURRENCY),
    //    it fails-fast and refuses to insert the intent. This prevents opening a useless transaction (`tx_pool.begin()`),
    //    saving precious database connections and network roundtrips. It gracefully sheds load 
    //    while allowing a controlled number of concurrent workers to proceed to the OCC phase.
    #[derive(sqlx::FromRow)]
    struct CcoResult {
        success: bool,
        collision_count: i64,
    }

    let row = sqlx::query_as::<_, CcoResult>(
        r#"
        WITH collisions AS (
            SELECT lock_key as colliding_key FROM lock_free.append_intents
            WHERE (events @? $2::jsonpath) OR ($3::jsonb @? context)
        ),
        zombie_check AS (
            SELECT colliding_key, pg_try_advisory_lock(colliding_key) as is_zombie
            FROM collisions
        ),
        active_collisions AS (
            SELECT colliding_key FROM zombie_check WHERE NOT is_zombie
        ),
        cleanup_zombies AS (
            DELETE FROM lock_free.append_intents
            WHERE lock_key IN (SELECT colliding_key FROM zombie_check WHERE is_zombie)
        ),
        release_zombie_locks AS (
            SELECT pg_advisory_unlock(colliding_key)
            FROM zombie_check WHERE is_zombie
        ),
        insertion AS (
            INSERT INTO lock_free.append_intents (intent_id, lock_key, context, events)
            SELECT $1, $4, $2::jsonpath, $3::jsonb 
            WHERE (SELECT count(*) FROM active_collisions) < $5
            RETURNING true as inserted
        )
        SELECT 
            COALESCE((SELECT inserted FROM insertion), false) as success,
            (SELECT count(*) FROM active_collisions) as collision_count
        "#
    )
    .bind(intent_id)
    .bind(&json_context)
    .bind(&event_payload)
    .bind(lock_key)
    .bind(MAX_CONTEXT_CONCURRENCY)
    .fetch_one(&mut *lock_conn)
    .await?;

    if !row.success {
        // Unlock before returning
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *lock_conn)
            .await?;
            
        if row.collision_count >= MAX_CONTEXT_CONCURRENCY {
            return Ok(AppendOutcome::ContextCollisionOnIntentRegistration);
        } else {
            return Ok(AppendOutcome::IntentRegistrationError);
        }
    }

    let mut tx = tx_pool.begin().await?;

    // 2. OCC Append. No session lock connection. No GC.
    //    If there is a false-positive (a zombie that should have been cleaned up during the super tiny time between 1. and 2.),
    //    that will only result in a retry. But that's super rare, and would be treated as a normal pessimistic collision situation.
    #[derive(sqlx::FromRow)]
    struct OccResult {
        inserted_seq: Option<i64>,
        actual_version: i64,
        _collisions: i64,
    }

    let result = sqlx::query_as::<_, OccResult>(
        r#"
        WITH expected AS (
            SELECT COALESCE(MAX(sequence_number), 0) as current_version
            FROM lock_free.events
            WHERE event_type = 'UserRegistered'
              AND payload @> jsonb_build_object('username', $1)
        ),
        registry_cross_check AS (
            SELECT count(*) as cnt 
            FROM lock_free.append_intents other
            JOIN lock_free.append_intents me ON me.intent_id = $5
            WHERE ((other.events @? $3::jsonpath) OR ($4::jsonb @? other.context))
              AND other.intent_id != $5
              AND other.lock_key < me.lock_key
        ),
        insertion AS (
            INSERT INTO lock_free.events (event_type, payload)
            SELECT 'UserRegistered', jsonb_build_object('username', $1)
            WHERE (SELECT current_version FROM expected) = $2
              AND (SELECT cnt FROM registry_cross_check) = 0
            RETURNING sequence_number
        )
        SELECT 
            (SELECT sequence_number FROM insertion) as inserted_seq,
            (SELECT current_version FROM expected) as actual_version,
            (SELECT cnt FROM registry_cross_check) as _collisions
        "#
    )
    .bind(username)
    .bind(expected_context_version)
    .bind(&json_context)
    .bind(&event_payload)
    .bind(intent_id)
    .fetch_one(&mut *tx)
    .await?;

    let occ_result = if let Some(seq) = result.inserted_seq {
        tx.commit().await?;
        Ok(AppendOutcome::Success(AppendSuccess { sequence_number: seq }))
    } else {
        tx.rollback().await?;
        
        // 4. Error Resolution (Why did it fail?)
        // The diagnostic was returned directly in the query result!
        // No need to query the database again.
        if result.actual_version == expected_context_version {
            // The version hasn't changed. No other transaction has committed a modification yet.
            // We failed purely because we lost the Tie-Breaker against a concurrent worker.
            Ok(AppendOutcome::ContextCollisionOnOptimisticCheck)
        } else {
            // The version HAS changed. We failed because our expected sequence number was stale.
            Ok(AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(ContextConflict {
                expected: expected_context_version,
                actual: result.actual_version,
            }))
        }
    };

    // 3. Self Garbage Collection (Explicitly release lock)
    // MUST happen AFTER commit/rollback to avoid the "invisible intent / invisible event" race condition.
    sqlx::query("DELETE FROM lock_free.append_intents WHERE intent_id = $1")
        .bind(intent_id)
        .execute(&mut *lock_conn)
        .await?;
        
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *lock_conn)
        .await?;

    occ_result
}

pub async fn serialized_append_batched(
    tx_pool: &PgPool,
    batch_json: serde_json::Value,
) -> sqlx::Result<Vec<AppendOutcome>> {
    let mut tx = tx_pool.begin().await?;

    let result = sqlx::query(
        r#"
        WITH incoming AS (
            SELECT 
                (elem->>'client_index')::int AS client_index,
                elem->>'username' AS username,
                (elem->>'expected_context_version')::bigint AS expected_context_version
            FROM jsonb_array_elements($1::jsonb) AS elem
        ),
        updated_meta AS (
            UPDATE serialized.metadata
            SET current_sequence_number = current_sequence_number + (SELECT count(*) FROM incoming)
            WHERE id = TRUE
            RETURNING current_sequence_number
        ),
        numbered_incoming AS (
            SELECT 
                i.client_index,
                i.username,
                i.expected_context_version,
                u.current_sequence_number - (SELECT count(*) FROM incoming) + row_number() OVER (ORDER BY i.client_index) AS seq
            FROM incoming i
            CROSS JOIN updated_meta u
        ),
        inserted AS (
            INSERT INTO serialized.events (sequence_number, event_type, payload)
            SELECT seq, 'UserRegistered', jsonb_build_object('username', username)
            FROM numbered_incoming
            RETURNING sequence_number
        )
        SELECT 1
        "#
    )
    .bind(batch_json.clone())
    .execute(&mut *tx)
    .await;

    match result {
        Ok(_) => {
            tx.commit().await?;
            let len = batch_json.as_array().unwrap().len();
            Ok(vec![AppendOutcome::Success(AppendSuccess { sequence_number: 1 }); len])
        }
        Err(e) => {
            tx.rollback().await?;
            Err(e)
        }
    }
}

pub async fn lock_free_append_mega_batch(
    tx_pool: &PgPool,
    lock_pool: &PgPool,
    requests: &[BatchRequest],
) -> sqlx::Result<Option<Vec<AppendOutcome>>> {
    const MAX_CONTEXT_CONCURRENCY: i64 = 10;
    
    if requests.is_empty() {
        return Ok(Some(Vec::new()));
    }

    // 1. Build Mega-Intent
    let intent_id = uuid::Uuid::new_v4();
    let lock_key: i64 = rand::random::<i64>();
    
    let mut username_conditions = Vec::with_capacity(requests.len());
    let mut events_payloads = Vec::with_capacity(requests.len());
    let mut requests_json = Vec::with_capacity(requests.len());
    
    for (i, req) in requests.iter().enumerate() {
        username_conditions.push(format!("@.username == \"{}\"", req.username));
        events_payloads.push(serde_json::json!({"username": req.username}));
        
        requests_json.push(serde_json::json!({
            "client_index": i,
            "username": req.username,
            "expected_context_version": req.expected_context_version
        }));
    }
    
    let combined_conditions = username_conditions.join(" || ");
    let mega_json_context = format!("$[*] ? ({})", combined_conditions);
    let mega_event_payload = serde_json::Value::Array(events_payloads);
    let batch_requests_json = serde_json::Value::Array(requests_json);

    let mut lock_conn = lock_pool.acquire().await?;

    // 2. Sieve Phase (Insert Mega-Intent)
    // Take the single lock
    sqlx::query("SELECT pg_advisory_lock($1)").bind(lock_key).execute(&mut *lock_conn).await?;

    #[derive(sqlx::FromRow)]
    struct SieveResult {
        success: bool,
    }

    let sieve_row = sqlx::query_as::<_, SieveResult>(
        r#"
        WITH collisions AS (
            SELECT lock_key as colliding_key FROM lock_free.append_intents
            WHERE (events @? $2::jsonpath) OR ($3::jsonb @? context)
        ),
        zombie_check AS (
            SELECT colliding_key, pg_try_advisory_lock(colliding_key) as is_zombie
            FROM collisions
        ),
        active_collisions AS (
            SELECT colliding_key FROM zombie_check WHERE NOT is_zombie
        ),
        cleanup_zombies AS (
            DELETE FROM lock_free.append_intents
            WHERE lock_key IN (SELECT colliding_key FROM zombie_check WHERE is_zombie)
        ),
        release_zombie_locks AS (
            SELECT pg_advisory_unlock(colliding_key)
            FROM zombie_check WHERE is_zombie
        ),
        insertion AS (
            INSERT INTO lock_free.append_intents (intent_id, lock_key, context, events)
            SELECT $1, $4, $2::jsonpath, $3::jsonb 
            WHERE (SELECT count(*) FROM active_collisions) < $5
            RETURNING true as inserted
        )
        SELECT 
            COALESCE((SELECT inserted FROM insertion), false) as success
        "#
    )
    .bind(intent_id)
    .bind(&mega_json_context)
    .bind(&mega_event_payload)
    .bind(lock_key)
    .bind(MAX_CONTEXT_CONCURRENCY)
    .fetch_one(&mut *lock_conn)
    .await?;

    if !sieve_row.success {
        sqlx::query("SELECT pg_advisory_unlock($1)").bind(lock_key).execute(&mut *lock_conn).await?;
        return Ok(None); // Sieve Failed -> Fallback
    }

    // 3. OCC Phase
    let mut tx = tx_pool.begin().await?;
    
    #[derive(sqlx::FromRow)]
    struct OccMegaResult {
        success: bool,
    }

    // We check if ANY of the expected versions are mismatched, or if there's a cross-check collision.
    let occ_check = sqlx::query_as::<_, OccMegaResult>(
        r#"
        WITH requests AS (
            SELECT 
                (elem->>'client_index')::int AS client_index,
                (elem->>'username') AS username,
                (elem->>'expected_context_version')::bigint AS expected_context_version
            FROM jsonb_array_elements($1::jsonb) AS elem
        ),
        expected AS (
            SELECT 
                r.username,
                r.expected_context_version,
                COALESCE(MAX(e.sequence_number), 0) as current_version
            FROM requests r
            LEFT JOIN lock_free.events e 
              ON e.event_type = 'UserRegistered' 
              AND e.payload @> jsonb_build_object('username', r.username)
            GROUP BY r.username, r.expected_context_version
        ),
        mismatches AS (
            SELECT 1 FROM expected
            WHERE current_version != expected_context_version
        ),
        duplicates AS (
            SELECT username FROM requests GROUP BY username HAVING count(*) > 1
        ),
        registry_cross_check AS (
            SELECT count(*) as cnt 
            FROM lock_free.append_intents other
            JOIN lock_free.append_intents me ON me.intent_id = $4
            WHERE ((other.events @? $2::jsonpath) OR ($3::jsonb @? other.context))
              AND other.intent_id != $4
              AND other.lock_key < me.lock_key
        )
        SELECT 
            (SELECT count(*) FROM mismatches) = 0 AND 
            (SELECT cnt FROM registry_cross_check) = 0 AND
            (SELECT count(*) FROM duplicates) = 0 as success
        "#
    )
    .bind(&batch_requests_json)
    .bind(&mega_json_context)
    .bind(&mega_event_payload)
    .bind(intent_id)
    .fetch_one(&mut *tx)
    .await?;

    let mega_result = if occ_check.success {
        // All checks passed! Insert all events.
        #[derive(sqlx::FromRow)]
        struct InsertedRow {
            sequence_number: i64,
        }
        
        let inserted_rows = sqlx::query_as::<_, InsertedRow>(
            r#"
            WITH requests AS (
                SELECT 
                    (elem->>'client_index')::int AS client_index,
                    (elem->>'username') AS username
                FROM jsonb_array_elements($1::jsonb) AS elem
            )
            INSERT INTO lock_free.events (event_type, payload)
            SELECT 'UserRegistered', jsonb_build_object('username', r.username)
            FROM requests r
            ORDER BY r.client_index
            RETURNING sequence_number
            "#
        )
        .bind(&batch_requests_json)
        .fetch_all(&mut *tx)
        .await?;
        
        tx.commit().await?;
        
        let outcomes: Vec<AppendOutcome> = inserted_rows.into_iter()
            .map(|r| AppendOutcome::Success(AppendSuccess { sequence_number: r.sequence_number }))
            .collect();
            
        Ok(Some(outcomes))
    } else {
        tx.rollback().await?;
        Ok(None) // OCC Failed -> Fallback
    };

    // 4. Garbage Collection
    sqlx::query("DELETE FROM lock_free.append_intents WHERE intent_id = $1")
        .bind(intent_id)
        .execute(&mut *lock_conn)
        .await?;
        
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *lock_conn)
        .await?;

    mega_result
}


pub async fn naive_atomic_cte_append(
    pool: &PgPool,
    username: &str,
    expected_context_version: i64,
) -> sqlx::Result<Option<AppendSuccess>> {
    let sequence_number = sqlx::query_scalar(
        r#"
        WITH context AS (
            SELECT COALESCE(MAX(sequence_number), 0) AS current_context_version
            FROM atomic_cte.events
            WHERE event_type = 'UserRegistered'
              AND payload @> jsonb_build_object('username', $1)
        )
        INSERT INTO atomic_cte.events (event_type, payload)
        SELECT
            'UserRegistered',
            jsonb_build_object('username', $1)
        FROM context
        WHERE current_context_version = $2
        RETURNING sequence_number
        "#,
    )
    .bind(username)
    .bind(expected_context_version)
    .fetch_optional(pool)
    .await?;

    Ok(sequence_number.map(|sequence_number| AppendSuccess { sequence_number }))
}

pub async fn naive_atomic_cte_context_version(pool: &PgPool, username: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COALESCE(MAX(sequence_number), 0)
        FROM atomic_cte.events
        WHERE event_type = 'UserRegistered'
          AND payload @> jsonb_build_object('username', $1)
        "#,
    )
    .bind(username)
    .fetch_one(pool)
    .await
}

pub async fn naive_atomic_cte_matching_event_count(pool: &PgPool, username: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM atomic_cte.events
        WHERE event_type = 'UserRegistered'
          AND payload @> jsonb_build_object('username', $1)
        "#,
    )
    .bind(username)
    .fetch_one(pool)
    .await
}

pub async fn install_naive_atomic_cte_test_gate(
    pool: &PgPool,
    advisory_lock_key: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION atomic_cte.wait_on_test_gate()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            PERFORM pg_advisory_xact_lock(TG_ARGV[0]::bigint);
            RETURN NEW;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query("DROP TRIGGER IF EXISTS wait_on_test_gate ON atomic_cte.events")
        .execute(pool)
        .await?;

    let create_trigger = format!(
        r#"
        CREATE TRIGGER wait_on_test_gate
        BEFORE INSERT ON atomic_cte.events
        FOR EACH ROW
        EXECUTE FUNCTION atomic_cte.wait_on_test_gate('{advisory_lock_key}')
        "#,
    );
    sqlx::query(&create_trigger).execute(pool).await?;

    Ok(())
}

pub async fn acquire_session_advisory_lock(
    connection: &mut PgConnection,
    advisory_lock_key: i64,
) -> sqlx::Result<()> {
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(advisory_lock_key)
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn release_session_advisory_lock(
    connection: &mut PgConnection,
    advisory_lock_key: i64,
) -> sqlx::Result<()> {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(advisory_lock_key)
        .execute(connection)
        .await?;
    Ok(())
}