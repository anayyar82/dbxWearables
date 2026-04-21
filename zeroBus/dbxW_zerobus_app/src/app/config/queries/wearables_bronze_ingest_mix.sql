-- Ingest mix by record type — optional type filter.
-- @param startDate DATE
-- @param endDate DATE
-- @param userPattern STRING
-- @param recordType STRING
SELECT
  record_type,
  SUM(row_count) AS row_count
FROM `users`.`ankur_nayyar`.`01_wearable_gold_bronze_ingest_daily`
WHERE ingest_day BETWEEN :startDate AND :endDate
  AND user_id LIKE :userPattern
  AND (
    :recordType = 'ALL'
    OR record_type = :recordType
  )
GROUP BY record_type
ORDER BY row_count DESC;
