-- Bronze ingest volume per calendar day (for timeline).
-- @param startDate DATE
-- @param endDate DATE
-- @param userPattern STRING
SELECT
  CAST(b.ingested_at AS DATE) AS ingest_day,
  COUNT(*) AS row_count
FROM users.ankur_nayyar.wearables_zerobus b
WHERE CAST(b.ingested_at AS DATE) BETWEEN :startDate AND :endDate
  AND b.user_id LIKE :userPattern
GROUP BY CAST(b.ingested_at AS DATE)
ORDER BY ingest_day;
