-- summarize_gold: Aggregate network summary by region and network type
-- Job: TelConnect CDR Pipeline > summarize_gold task
-- Warehouse: 8c717a440d022dc0

SELECT region, network_type, total_calls, drop_rate_pct
FROM telconnect_lab.gold.network_summary
ORDER BY drop_rate_pct DESC;