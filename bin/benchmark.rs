use ccc_postgres_concurrency_proof::{
    connect_pool, lock_free_append, reset_database, serialized_append, AppendOutcome, DEFAULT_DATABASE_URL,
};

// Default URL for the connection pool dedicated to session locks
pub const DEFAULT_LOCK_DATABASE_URL: &str =
    "postgres://postgres:postgres@localhost:6433/concurrency_proof";

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Barrier;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("========================================================");
    println!("  CCC Postgres Concurrency Proof - Scaling Benchmark");
    println!("========================================================");
    
    // Retrieve connection URLs from the environment, or use defaults
    let tx_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_string());
    let lock_url = std::env::var("LOCK_DATABASE_URL").unwrap_or_else(|_| DEFAULT_LOCK_DATABASE_URL.to_string());

    // Create two distinct connection pools
    // tx_pool is used for standard queries (transaction mode)
    let tx_pool = connect_pool(&tx_url, "benchmark_tx", 65000).await?;
    // lock_pool is used exclusively for acquiring pg_advisory_lock (session mode required)
    let lock_pool = connect_pool(&lock_url, "benchmark_lock", 100).await?;

    println!("Warming up connection pools (target: 60,000 TCP connections per pool)...");
    let mut conns = Vec::with_capacity(125_000);

    // Warmup TX Pool: Pre-warm the transactional pool
    // The goal is to open all TCP connections in advance to avoid 
    // polluting performance metrics with TCP handshake latency.
    let mut success_tx = 0;
    while success_tx < 60_000 {
        let mut handles = Vec::with_capacity(500);
        // Launch 500 connection attempts in parallel
        for _ in 0..500 {
            let t_pool = tx_pool.clone();
            handles.push(tokio::spawn(async move { t_pool.acquire().await }));
        }
        // Wait for the 500 connections to be established
        for handle in handles {
            if let Ok(Ok(c)) = handle.await {
                conns.push(c); // Keep the connection open by storing it
                success_tx += 1;
            }
        }
        print!("\rWarmed up TX pool: {}/60000", success_tx);
        use std::io::Write;
        std::io::stdout().flush().unwrap();
        // Short pause to avoid overwhelming the local network at once
        if success_tx < 60_000 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    println!();
    
    // Warmup LOCK Pool: Pre-warm the lock pool
    let mut success_lock = 0;
    while success_lock < 100 {
        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            let l_pool = lock_pool.clone();
            handles.push(tokio::spawn(async move { l_pool.acquire().await }));
        }
        for handle in handles {
            if let Ok(Ok(c)) = handle.await {
                conns.push(c); // Keep the connection open by storing it
                success_lock += 1;
            }
        }
        print!("\rWarmed up LOCK pool: {}/100", success_lock);
        use std::io::Write;
        std::io::stdout().flush().unwrap();
        if success_lock < 100 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    println!();

    // Warmup LOCK Pool: Pre-warm the session pool (for advisory locks)
    // Same principle, ensuring PgBouncer has allocated the necessary sockets.
    let mut success_lock = 50_000;
    while success_lock < 50_000 {
        let mut handles = Vec::with_capacity(500);
        for _ in 0..500 {
            let l_pool = lock_pool.clone();
            handles.push(tokio::spawn(async move { l_pool.acquire().await }));
        }
        for handle in handles {
            if let Ok(Ok(c)) = handle.await {
                conns.push(c);
                success_lock += 1;
            }
        }
        print!("\rWarmed up LOCK pool: {}/50000", success_lock);
        use std::io::Write;
        std::io::stdout().flush().unwrap();
        if success_lock < 50_000 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
    println!();
    
    // By clearing the `conns` vector, we release all acquired connections
    // They return to their respective pools (Idle status), ready for instant use
    drop(conns);
    println!("Connection pools warmed up.\n");

    // Loop through the different concurrency levels to test
    // Run scale testing
    let levels = vec![100, 500, 1000, 2500, 5000, 7500, 10000, 20000, 30000, 40000, 50000];
    for concurrency in levels {
        println!("\n>>> Testing with {} Concurrent Tasks <<<", concurrency);

        // --- Scenario 3: Serialized - Low Contention ---
        // Reset the DB to start fresh
        reset_database(&tx_pool).await?;
        println!("--- Serialized Append (High Contention/Same Context) ---");
        run_benchmark(
            &tx_pool,
            &lock_pool,
            "user_".to_string(),
            true, // true = High Contention (queries target different users)
            concurrency,
            |p_tx, _p_lock, u, v| Box::pin(async move { serialized_append(&p_tx, &u, v).await }),
        )
        .await?;

        // --- Scenario 4: Lock-Free - Low Contention ---
        reset_database(&tx_pool).await?;
        println!("--- Lock-Free (High Contention/Same Context) ---");
        run_benchmark(
            &tx_pool,
            &lock_pool,
            "user_".to_string(),
            true,
            concurrency,
            |p_tx, p_lock, u, v| Box::pin(async move { lock_free_append(&p_tx, &p_lock, &u, v).await }),
        )
        .await?;



        // --- Scenario 7: Lock-Free Batched - Low Contention ---
        reset_database(&tx_pool).await?;
        println!("--- Lock-Free Batched (High Contention/Same Context) ---");
        let batcher_indep = ccc_postgres_concurrency_proof::OptimisticBatcher::new(tx_pool.clone(), lock_pool.clone());
        run_benchmark(
            &tx_pool,
            &lock_pool,
            "user_".to_string(),
            true,
            concurrency,
            move |_p_tx, _p_lock, u, v| {
                let b = batcher_indep.clone();
                Box::pin(async move { b.append(u, v).await })
            },
        )
        .await?;


    }

    Ok(())
}

// Generic utility function to run benchmarks
async fn run_benchmark<F>(
    tx_pool: &sqlx::PgPool,
    lock_pool: &sqlx::PgPool,
    base_username: String,
    high_contention: bool,
    concurrency: usize,
    append_fn: F, // Closure containing the append function (serialized or lock_free)
) -> anyhow::Result<()>
where
    // Type constraints for the closure (must return a Future)
    F: Fn(sqlx::PgPool, sqlx::PgPool, String, i64) -> std::pin::Pin<Box<dyn std::future::Future<Output = sqlx::Result<AppendOutcome>> + Send>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    // Using a Barrier to ensure all tasks start at the exact same
    // microsecond (simulates a perfect instant traffic spike)
    let barrier = Arc::new(Barrier::new(concurrency));
    
    // Atomic counters (thread-safe) to gather statistics
    let success_count = Arc::new(AtomicUsize::new(0));
    let conflict_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let rate_limit_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(concurrency);

    // Prepare all asynchronous tasks
    for i in 0..concurrency {
        // Clone Arc references for each task
        let tx_pool = tx_pool.clone();
        let lock_pool = lock_pool.clone();
        let barrier = barrier.clone();
        let append_fn = append_fn.clone();
        let success_count = success_count.clone();
        let conflict_count = conflict_count.clone();
        let error_count = error_count.clone();
        let rate_limit_count = rate_limit_count.clone();

        // In high contention, everyone targets "user_".
        // In low contention, each task targets its own "user_i".
        let username = if high_contention {
            base_username.clone()
        } else {
            format!("{}{}", base_username, i)
        };

        handles.push(tokio::spawn(async move {
            // All tasks pause here.
            // As soon as the last task reaches this point, the barrier drops
            // and everyone fires off at the exact same time.
            barrier.wait().await;
            
            // Expected context is always 0 for this test (since it's a fresh DB or independent users)
            match append_fn(tx_pool, lock_pool, username, 0).await {
                Ok(AppendOutcome::Success(_)) => {
                    success_count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(AppendOutcome::SequenceNumberInconsistencyOnOptimisticCheck(_)) => {
                    // OCC Conflict: The transaction reached the end but the sequence number did not match
                    conflict_count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(AppendOutcome::ContextCollisionOnOptimisticCheck) => {
                    // Rejection by the Hybrid OCC (the request was blocked by an older request)
                    rate_limit_count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(AppendOutcome::ContextCollisionOnIntentRegistration) => {
                    // Fast-fail rejection by the CCO (Load shedding) due to too many concurrent requests on the same context
                    rate_limit_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(AppendOutcome::IntentRegistrationError) => {
                    // Rejection by the Hybrid OCC (the request was blocked by an older request)
                    rate_limit_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    // Unexpected error (e.g., PoolTimedOut, Database crash)
                    println!("ERROR: {:?}", e);
                    error_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // The stopwatch only starts when we begin waiting for execution to finish
    let start = Instant::now();
    for handle in handles {
        let _ = handle.await;
    }
    let duration = start.elapsed(); // Stop the stopwatch once all requests are processed

    // Retrieve the totals
    let successes = success_count.load(Ordering::Relaxed);
    let conflicts = conflict_count.load(Ordering::Relaxed);
    let rate_limits = rate_limit_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    
    // Calculate Transactions Per Second (TPS)
    let tps = (concurrency as f64 / duration.as_secs_f64()) as usize;

    println!("  Duration: {:?}", duration);
    println!("  TPS:      {} req/s", tps);
    println!(
        "  Results:  {} Successes, {} Conflicts, {} Rate Limits, {} Errors",
        successes, conflicts, rate_limits, errors
    );
    println!();

    Ok(())
}
