use ccc_postgres_concurrency_proof::{connect_pool, lock_free_append, DEFAULT_DATABASE_URL};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::time::Instant;


async fn reset_database(pool: &PgPool) -> sqlx::Result<()> {
    sqlx::query("TRUNCATE TABLE lock_free.events RESTART IDENTITY CASCADE;")
        .execute(pool)
        .await?;
    sqlx::query("CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$ 
                 BEGIN 
                   PERFORM pg_notify('lock_free_events', NEW.sequence_number::text); 
                   RETURN NEW; 
                 END; $$ LANGUAGE plpgsql;").execute(pool).await?;
    sqlx::query("DROP TRIGGER IF EXISTS notify_event_trigger ON lock_free.events").execute(pool).await?;
    sqlx::query("CREATE TRIGGER notify_event_trigger AFTER INSERT ON lock_free.events FOR EACH ROW EXECUTE PROCEDURE notify_event()").execute(pool).await?;
    Ok(())
}

async fn run_producer(tx_pool: PgPool, lock_pool: PgPool, target_events: i64) -> sqlx::Result<()> {
    println!("Producer: Starting {} appends...", target_events);
    let barrier = Arc::new(Barrier::new(target_events as usize + 1));
    let mut handles = Vec::with_capacity(target_events as usize);

    for i in 0..target_events {
        let t_pool = tx_pool.clone();
        let l_pool = lock_pool.clone();
        let b = barrier.clone();
        
        handles.push(tokio::spawn(async move {
            b.wait().await;
            // High contention but distinct contexts to simulate massive parallel writes
            let context = format!("user_{}", i);
            let _ = lock_free_append(&t_pool, &l_pool, &context, 0).await;
        }));
    }

    let start = Instant::now();
    barrier.wait().await; // Release the kraken!

    for handle in handles {
        let _ = handle.await;
    }
    
    println!("Producer: Finished in {:?}", start.elapsed());
    Ok(())
}



async fn model_b_long_ttl(pool: PgPool, _reader_id: usize, target_events: i64) -> sqlx::Result<(std::time::Duration, i64, i32, i64, i32)> {
    let mut last_seen = 0;
    let mut stutters = 0;
    let mut sum_seq = 0;
    let mut missed_events = 0;
    let start = Instant::now();
    let max_ttl = std::time::Duration::from_secs(60);
    let mut current_gap: Option<(i64, Instant)> = None;

    let _ = tokio::time::timeout(std::time::Duration::from_secs(180), async {
        loop {
            let rows: Vec<(i64,)> = sqlx::query_as("SELECT sequence_number FROM lock_free.events WHERE sequence_number > $1 ORDER BY sequence_number ASC LIMIT 1000")
                .bind(last_seen)
                .fetch_all(&pool)
                .await.unwrap_or_default();

            if rows.is_empty() {
                let max_seq: (Option<i64>,) = sqlx::query_as("SELECT MAX(sequence_number) FROM lock_free.events").fetch_one(&pool).await.unwrap_or((None,));
                if max_seq.0.unwrap_or(0) >= target_events && last_seen >= target_events {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }

            let mut gap_hit = false;
            for (seq,) in rows {
                if seq != last_seen + 1 {
                    gap_hit = true;
                    let expected_seq = last_seen + 1;
                    let mut ttl_expired = false;
                    
                    if let Some((gap_seq, gap_start)) = current_gap {
                        if gap_seq == expected_seq {
                            if gap_start.elapsed() > max_ttl {
                                ttl_expired = true;
                            }
                        } else {
                            current_gap = Some((expected_seq, Instant::now()));
                        }
                    } else {
                        current_gap = Some((expected_seq, Instant::now()));
                    }

                    if ttl_expired {
                        missed_events += 1;
                        last_seen += 1;
                        current_gap = None;
                        break;
                    } else {
                        stutters += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        break;
                    }
                } else {
                    last_seen = seq;
                    sum_seq += seq;
                    current_gap = None;
                }
            }

            if last_seen >= target_events {
                break;
            }
            
            if !gap_hit {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
    }).await;

    Ok((start.elapsed(), last_seen, stutters, sum_seq, missed_events))
}

async fn model_c_short_ttl_ratchet(pool: PgPool, _reader_id: usize, target_events: i64) -> sqlx::Result<(std::time::Duration, i64, i32, i64, i32)> {
    let mut last_seen = 0;
    let mut stutters = 0;
    let mut sum_seq = 0;
    let mut missed_events = 0;
    let start = Instant::now();
    let max_ttl = std::time::Duration::from_millis(1000);
    let mut current_gap: Option<(i64, Instant)> = None;

    let _ = tokio::time::timeout(std::time::Duration::from_secs(180), async {
        loop {
            let rows: Vec<(i64,)> = sqlx::query_as(
                "SELECT sequence_number FROM lock_free.events 
                 WHERE sequence_number > $1 
                 AND xmin::text::bigint < (pg_snapshot_xmin(pg_current_snapshot()))::text::bigint
                 ORDER BY sequence_number ASC LIMIT 1000"
            )
                .bind(last_seen)
                .fetch_all(&pool)
                .await.unwrap_or_default();

            if rows.is_empty() {
                let max_seq: (Option<i64>,) = sqlx::query_as("SELECT MAX(sequence_number) FROM lock_free.events").fetch_one(&pool).await.unwrap_or((None,));
                if max_seq.0.unwrap_or(0) >= target_events && last_seen >= target_events {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }

            let mut gap_hit = false;
            for (seq,) in rows {
                if seq != last_seen + 1 {
                    gap_hit = true;
                    let expected_seq = last_seen + 1;
                    let mut ttl_expired = false;
                    
                    if let Some((gap_seq, gap_start)) = current_gap {
                        if gap_seq == expected_seq {
                            if gap_start.elapsed() > max_ttl {
                                ttl_expired = true;
                            }
                        } else {
                            current_gap = Some((expected_seq, Instant::now()));
                        }
                    } else {
                        current_gap = Some((expected_seq, Instant::now()));
                    }

                    if ttl_expired {
                        missed_events += 1;
                        last_seen += 1;
                        current_gap = None;
                        break;
                    } else {
                        stutters += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        break;
                    }
                } else {
                    last_seen = seq;
                    sum_seq += seq;
                    current_gap = None;
                }
            }

            if last_seen >= target_events {
                break;
            }
            
            if !gap_hit {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }).await;

    Ok((start.elapsed(), last_seen, stutters, sum_seq, missed_events))
}

async fn model_d_logical_decoding(pool: PgPool, reader_id: usize, target_events: i64) -> sqlx::Result<(std::time::Duration, i64, i32, i64, i32)> {
    let slot_name = format!("bench_slot_{}", reader_id);
    let _ = sqlx::query(&format!("SELECT pg_drop_replication_slot('{}')", slot_name)).execute(&pool).await;
    sqlx::query(&format!("SELECT pg_create_logical_replication_slot('{}', 'test_decoding')", slot_name)).execute(&pool).await?;
    
    let mut received = 0;
    let stutters = 0;
    let mut sum_seq = 0;
    let start = Instant::now();

    let _ = tokio::time::timeout(std::time::Duration::from_secs(180), async {
        loop {
            let changes: Vec<(String, String, String)> = sqlx::query_as(
                &format!("SELECT lsn::text, xid::text, data FROM pg_logical_slot_get_changes('{}', NULL, NULL)", slot_name)
            )
                .fetch_all(&pool)
                .await.unwrap_or_default();

            if changes.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue;
            }

            for (_, _, data) in changes {
                if data.starts_with("table \"lock_free\".\"events\": INSERT:") || (data.contains("INSERT") && data.contains("events")) {
                    received += 1;
                    if let Some(idx) = data.find("sequence_number[bigint]:") {
                        let substr = &data[idx + 24..];
                        if let Some(space_idx) = substr.find(' ') {
                            if let Ok(seq) = substr[..space_idx].parse::<i64>() {
                                sum_seq += seq;
                            }
                        }
                    }
                }
            }

            if received >= target_events {
                break;
            }
        }
    }).await;

    Ok((start.elapsed(), received, stutters, sum_seq, 0))
}

#[tokio::main]
async fn main() -> sqlx::Result<()> {
    println!("========================================================");
    println!("  CCC Postgres Concurrency Proof - Read Benchmark");
    println!("========================================================");

    let tx_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_string());
    let lock_url = std::env::var("LOCK_DATABASE_URL").unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/concurrency_proof".to_string());
    let reader_url = std::env::var("READER_DATABASE_URL").unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/concurrency_proof".to_string());
    let target_events: i64 = std::env::var("TARGET_EVENTS").unwrap_or_else(|_| "5000".to_string()).parse().unwrap();
    let num_readers: usize = std::env::var("NUM_READERS").unwrap_or_else(|_| "10".to_string()).parse().unwrap();

    let tx_pool = connect_pool(&tx_url, "benchmark_tx", target_events as u32).await?;
    let lock_pool = connect_pool(&lock_url, "benchmark_lock", 100).await?;
    let reader_pool = connect_pool(&reader_url, "benchmark_reader", num_readers as u32).await?;

    let mut conns = Vec::with_capacity(target_events as usize);
    
    // Warmup TX Pool
    let mut success_tx = 0;
    let mut attempts = 0;
    while success_tx < target_events {
        let batch_size = std::cmp::min(500, target_events - success_tx);
        let mut handles = Vec::with_capacity(batch_size as usize);
        for _ in 0..batch_size {
            let t_pool = tx_pool.clone();
            handles.push(tokio::spawn(async move { t_pool.acquire().await }));
        }
        for handle in handles {
            match handle.await {
                Ok(Ok(c)) => {
                    conns.push(c);
                    success_tx += 1;
                }
                Ok(Err(e)) => {
                    if attempts == 0 { println!("TX Warmup error: {:?}", e); }
                }
                Err(e) => {
                    if attempts == 0 { println!("TX Warmup panic: {:?}", e); }
                }
            }
        }
        attempts += 1;
        if attempts > 1000 {
            panic!("Failed to warmup TX pool. success_tx: {}", success_tx);
        }
        if success_tx < target_events { tokio::time::sleep(std::time::Duration::from_millis(10)).await; }
        print!("\rWarmed up TX pool: {}/{}", success_tx, target_events);
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    
    // Warmup LOCK Pool
    let mut success_lock = 0;
    attempts = 0;
    let target_lock = 100; // Hardcoded because we now restrict pgbouncer-lock to 100
    while success_lock < target_lock {
        let batch_size = std::cmp::min(100, target_lock - success_lock);
        let mut handles = Vec::with_capacity(batch_size as usize);
        for _ in 0..batch_size {
            let l_pool = lock_pool.clone();
            handles.push(tokio::spawn(async move { l_pool.acquire().await }));
        }
        for handle in handles {
            match handle.await {
                Ok(Ok(c)) => {
                    conns.push(c);
                    success_lock += 1;
                }
                Ok(Err(e)) => {
                    if attempts == 0 { println!("LOCK Warmup error: {:?}", e); }
                }
                Err(e) => {
                    if attempts == 0 { println!("LOCK Warmup panic: {:?}", e); }
                }
            }
        }
        attempts += 1;
        if attempts > 100 {
            panic!("Failed to warmup LOCK pool. success_lock: {}", success_lock);
        }
        if success_lock < target_lock { tokio::time::sleep(std::time::Duration::from_millis(10)).await; }
        print!("\rWarmed up LOCK pool: {}/{}", success_lock, target_lock);
        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
    drop(conns);
    println!("Pools warmed up!");
    

    
    // Model B
    println!("\n=============================================");
    println!("--- Model B: Gap Detector (Long TTL) [{} Consumers] ---", num_readers);
    reset_database(&tx_pool).await?;
    let mut readers = Vec::new();
    for i in 0..num_readers {
        let r_pool = reader_pool.clone();
        readers.push(tokio::spawn(async move { model_b_long_ttl(r_pool, i, target_events).await }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = run_producer(tx_pool.clone(), lock_pool.clone(), target_events).await;
    
    let expected_sum = (target_events * (target_events + 1)) / 2;
    let mut total_stutters = 0;
    let mut total_missed = 0;
    let mut max_dur = std::time::Duration::from_secs(0);
    let mut all_verified = true;
    let mut sums = Vec::new();
    for reader in readers {
        if let Ok(Ok((dur, _rec, st, sum_seq, missed))) = reader.await {
            total_missed += missed;
            total_stutters += st;
            sums.push(sum_seq);
            if dur > max_dur { max_dur = dur; }
            if sum_seq != expected_sum { all_verified = false; }
        }
    }
    let verif_str = format!("Expected: {}, Got: {:?}", expected_sum, sums);
    println!("Consumers B finished. Max Time: {:?}. Total Stutters: {}. Missed: {}. {}", max_dur, total_stutters, total_missed, verif_str);

    // Model C
    println!("\n=============================================");
    println!("--- Model C: Gap Detector + Ratchet (Short TTL) [{} Consumers] ---", num_readers);
    reset_database(&tx_pool).await?;
    let mut readers = Vec::new();
    for i in 0..num_readers {
        let r_pool = reader_pool.clone();
        readers.push(tokio::spawn(async move { model_c_short_ttl_ratchet(r_pool, i, target_events).await }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = run_producer(tx_pool.clone(), lock_pool.clone(), target_events).await;
    
    let mut total_stutters = 0;
    let mut total_missed = 0;
    let mut max_dur = std::time::Duration::from_secs(0);
    let mut all_verified = true;
    let mut sums = Vec::new();
    for reader in readers {
        if let Ok(Ok((dur, _rec, st, sum_seq, missed))) = reader.await {
            total_missed += missed;
            total_stutters += st;
            sums.push(sum_seq);
            if dur > max_dur { max_dur = dur; }
            if sum_seq != expected_sum { all_verified = false; }
        }
    }
    let verif_str = format!("Expected: {}, Got: {:?}", expected_sum, sums);
    println!("Consumers C finished. Max Time: {:?}. Total Stutters: {}. Missed: {}. {}", max_dur, total_stutters, total_missed, verif_str);

    // Model D
    println!("\n=============================================");
    println!("--- Model D: Logical Decoding (LSN) [{} Consumers] ---", num_readers);
    reset_database(&tx_pool).await?;
    // Create replication slots for all readers before spawning to avoid races
    let mut readers = Vec::new();
    for i in 0..num_readers {
        let r_pool = reader_pool.clone();
        readers.push(tokio::spawn(async move { model_d_logical_decoding(r_pool, i, target_events).await }));
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = run_producer(tx_pool.clone(), lock_pool.clone(), target_events).await;
    
    let mut total_stutters = 0;
    let mut total_missed = 0;
    let mut max_dur = std::time::Duration::from_secs(0);
    let mut all_verified = true;
    let mut sums = Vec::new();
    for reader in readers {
        if let Ok(Ok((dur, _rec, st, sum_seq, missed))) = reader.await {
            total_missed += missed;
            total_stutters += st;
            sums.push(sum_seq);
            if dur > max_dur { max_dur = dur; }
            if sum_seq != expected_sum { all_verified = false; }
        }
    }
    let verif_str = format!("Expected: {}, Got: {:?}", expected_sum, sums);
    println!("Consumers D finished. Max Time: {:?}. Total Stutters: {}. Missed: {}. {}", max_dur, total_stutters, total_missed, verif_str);

    Ok(())
}
