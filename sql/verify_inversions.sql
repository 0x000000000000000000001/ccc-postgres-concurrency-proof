-- Verifies txid vs sequence number inversions
WITH all_events AS (
  SELECT sequence_number, xmin::text::bigint as txid, 'serialized' as source FROM serialized.events
  UNION ALL
  SELECT sequence_number, xmin::text::bigint as txid, 'predicate_lock' as source FROM predicate_lock.events
),
ordered AS (
  SELECT source, sequence_number, txid, 
         lag(txid) OVER (PARTITION BY source ORDER BY sequence_number) as prev_txid
  FROM all_events
)
SELECT 
  source,
  COUNT(*) as total_events,
  SUM(CASE WHEN txid < prev_txid THEN 1 ELSE 0 END) as inversion_count,
  ROUND(SUM(CASE WHEN txid < prev_txid THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as inversion_percentage
FROM ordered
GROUP BY source;
