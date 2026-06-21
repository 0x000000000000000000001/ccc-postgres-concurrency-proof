use sqlx::{postgres::PgPoolOptions, PgConnection, PgPool, Postgres, Transaction};
use std::time::Duration;

pub const DEFAULT_DATABASE_URL: &str =
    "postgres://postgres:postgres@localhost:54329/concurrency_proof";

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
    ContextConflict(ContextConflict),
}

pub async fn connect_pool(database_url: &str, application_name: &str) -> sqlx::Result<PgPool> {
    let url = with_application_name(database_url, application_name);
    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
}

pub async fn reset_database(pool: &PgPool) -> sqlx::Result<()> {
    for migration in [
        include_str!("../sql/001_atomic_cte.sql"),
        include_str!("../sql/002_serialized.sql"),
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

pub async fn atomic_cte_context_version(pool: &PgPool, username: &str) -> sqlx::Result<i64> {
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

pub async fn atomic_cte_matching_event_count(pool: &PgPool, username: &str) -> sqlx::Result<i64> {
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

pub async fn atomic_cte_append(
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
        return Ok(AppendOutcome::ContextConflict(ContextConflict {
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

pub async fn install_atomic_cte_test_gate(
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

fn with_application_name(database_url: &str, application_name: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}application_name={application_name}")
}
